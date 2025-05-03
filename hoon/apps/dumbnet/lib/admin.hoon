/=  *  /common/zeke
/=  dk  /apps/dumbnet/lib/types
/=  nock-common  /common/nock-common
::
|_  a=admin-state:dk
++  produce-stark-config
  ^-  admin-state:dk
  ?^  prep.stark-config.a  a
  =/  in=stark-input
    %*  .
        *stark-input
        all-verifier-funcs
        all-verifier-funcs:nock-common
    ==
  a(prep.stark-config (some ~(preprocess-data stark-engine in)))
--
