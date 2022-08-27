##

"""
I want to find ordinary firms by 2 things:
  1. Their market as derived from their page title
  2. Their group name
- I remove 'pazireh' ones.
- I map multiple tickers for a same firm to its last ticker.
  """

import asyncio

import pandas as pd
from githubdata import GithubData
from mirutil import async_requests as areq
from mirutil import tsetmc as mt
from mirutil import utils as mu
from mirutil.df_utils import read_data_according_to_type as rdata
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.df_utils import save_df_as_a_nice_xl as snxl


btic_repo_url = 'https://github.com/imahdimir/d-BaseTicker'
t2m_url = 'https://github.com/imahdimir/d-MarketTitle-2-Market-ID-map'
mb2ft_url = 'https://github.com/imahdimir/d-multi-BaseTickers-2-same-FirmTicker-map'
id2ft_url = 'https://github.com/imahdimir/d-TSETMC-ID-2-FirmTicker-map'

tic = 'Ticker'
ftic = 'FirmTicker'
btic = 'BaseTicker'
skey = 'srchkey'
name = 'Name'
mkt = 'Market'
mktn = 'MarketName'
id1 = 'ID-1'
isact = 'IsActive'
titl = 'Title'
gpn = 'GroupName'
tmkt = titl + '.Mkt'
mktitl = 'MarketTitle'
mktid = 'MarketId'
isfirm = 'isfirm'
tid = 'TSETMC_ID'


def main() :
  pass

  ##


  bt_repo = GithubData(btic_repo_url)
  bt_repo.clone()

  ##
  dfbp = bt_repo.data_filepath
  dfb = rdata(dfbp)
  dfb[btic] = dfb[btic].str.strip()

  ##
  cis = mu.return_clusters_indices(dfb)

  ##
  dfb = dfb.reset_index(drop = True)
  dfs = pd.DataFrame()

  for se in cis :
    print(se)
    si = se[0]
    ei = se[1]

    btics = dfb.loc[si :ei , btic]

    _df = asyncio.run(mt.search_tsetmc_async(btics))

    dfs = pd.concat([dfs , _df])

  ##
  # keep only those which are exactly on testmc
  msk = dfs[tic].eq(dfs[skey])
  dfs = dfs[msk]

  ##
  # make sure every baseticker is in the searched ones
  msk = dfs[tic].isin(dfb[btic])
  assert msk.all()

  ##
  # keep id-1 and some other info
  dfs = dfs[[id1 , tic , name , mkt , isact]]

  ##
  cis = mu.return_clusters_indices(dfs)

  ##
  dfs['url'] = dfs[id1].apply(mt.make_tsetmc_overview_pg_url_with_testmc_id)

  ##
  dfs = dfs.reset_index(drop = True)

  for se in cis :
    print(se)

    si = se[0]
    ei = se[1]

    urls = dfs.loc[si : ei , 'url']

    out = asyncio.run(areq.get_reps_texts_async(urls))

    dfs.loc[si : ei , 'resptxt'] = out

  ##
  dfs[titl] = dfs['resptxt'].apply(mt.get_title_fr_resp_text)
  dfs[gpn] = dfs['resptxt'].apply(mt.get_group_name_fr_resp_text)

  ##
  sprq(dfs , 'temp2.prq')

  ##
  dfs = pd.read_parquet('temp2.prq')
  ##
  dfs = rdata('temp2.prq')

  ##
  dfs['tlen'] = dfs[titl].apply(lambda x : len(x))
  dfs['glen'] = dfs[gpn].apply(lambda x : len(x))
  ##
  assert dfs['tlen'].eq(1).all()
  ##
  assert dfs['glen'].eq(1).all()
  ##
  dfs = dfs.drop(columns = ['tlen' , 'glen'])
  ##
  dfs[titl] = dfs[titl].apply(lambda x : x[0])
  dfs[gpn] = dfs[gpn].apply(lambda x : x[0])
  ##
  dfs[tmkt] = dfs[titl].apply(mt.extract_market_from_tsetmc_title)  ##
  ##
  t2m_rp = GithubData(t2m_url)
  t2m_rp.clone()
  ##
  dfmfp = t2m_rp.data_filepath
  dfm = rdata(dfmfp)
  ##
  dfm = dfm.set_index(mktitl)
  ##
  dfs[mktid] = dfs[tmkt].map(dfm[mktid])
  ##
  msk = dfs[tmkt].eq('')
  msk |= dfs[tmkt].isna()
  assert dfs.loc[msk , mktid].isna().all()
  ##
  firm_mkts = dfs[[mktid]].drop_duplicates()
  for el in firm_mkts[mktid] :
    print(f'"{el}":None,')
  ##
  firm_mkts = {
      "TSE.Naghd.M1.Main"       : None ,
      "TSE.Naghd.M1.Subsidiary" : None ,
      "TSE.Naghd.M2"            : None ,
      "TSE.M4"                  : None ,
      "IFB.M1"                  : None ,
      "IFB.M2"                  : None ,
      "IFB.M3"                  : None ,
      "IFB.Paye"                : None ,
      "IFB.Paye.Yellow"         : None ,
      "IFB.Paye.Orange"         : None ,
      "IFB.Paye.Red"            : None ,
      "IFB.SME"                 : None ,
      }

  ##
  dfs[isfirm] = None
  msk = dfs[mktid].isin(firm_mkts.keys())
  dfs.loc[msk , isfirm] = True
  ##
  msk = dfs[isfirm].eq(True)
  gpsval = dfs.loc[msk , [gpn]].drop_duplicates()
  ##
  for el in gpsval[gpn] :
    print(f'"{el}":None,')
  ##
  not_firms_gp = {
      "اوراق بهادار مبتني بر دارايي فكري" : None ,
      }
  ##
  msk = ~ dfs[gpn].isin(not_firms_gp.keys())
  dfs.loc[msk , isfirm] = True
  ##
  msk = dfs[isfirm].eq(True)
  df1 = dfs[msk]

  ##
  # hazfe pazire ha
  ptr = r'\b' + 'پذيره' + r'\b'
  msk = dfs[tic].str.contains(ptr)
  df1 = dfs[msk]

  ##
  pzrs = df1[[tic]].drop_duplicates()
  for el in pzrs[tic] :
    print(f'"{el}":None,')

  ##
  not_firm_has_pzrs = {
      "باران-پذيره"  : None ,
      "تفارس-پذيره"  : None ,
      "تماوند-پذيره" : None ,
      "حگردش-پذيره"  : None ,
      "وهنر-پذيره"   : None ,
      "پاسار-پذيره"  : None ,
      "پذيره-بهشت"   : None ,
      "پذيره-ستون"   : None ,
      }
  ##
  assert df1[tic].isin(not_firm_has_pzrs.keys()).all()
  ##
  dfs.loc[msk , isfirm] = False
  ##
  msk = dfs[tic].str.contains('پذيره')
  msk &= dfs[isfirm].ne(False)
  df1 = dfs[msk]
  ##
  msk = dfs[isfirm].eq(True)
  df1 = dfs[msk]

  ##
  msk = dfs[isfirm].eq(True)
  dfs.loc[msk , ftic] = dfs[tic]

  ##
  msk = dfs[isfirm].eq(True)
  dfo = dfs.loc[msk , [id1 , ftic]]
  rname = {
      id1 : tid ,
      }
  dfo = dfo.rename(columns = rname)

  ##
  rp_m2f = GithubData(mb2ft_url)
  rp_m2f.clone()

  ##
  dfzfp = rp_m2f.data_filepath
  dfz = rdata(dfzfp)
  dfz[tid] = dfz[tid].astype(str)
  dfz = dfz.set_index(tid)

  ##
  dfo['ft'] = dfo[tid].map(dfz[ftic])
  msk = dfo['ft'].notna()
  df1 = dfo[msk]
  dfo.loc[msk , ftic] = dfo['ft']

  ##
  dfo = dfo[[tid , ftic]]

  ##
  rp_i2f = GithubData(id2ft_url)
  rp_i2f.clone()

  ##
  dfifp = rp_i2f.data_filepath

  ##
  snxl(dfo , dfifp)

  ##
  msg = 'init'
  rp_i2f.commit_push(msg)

  ##

  bt_repo.rmdir()
  t2m_rp.rmdir()
  rp_m2f.rmdir()
  rp_i2f.rmdir()

##

##