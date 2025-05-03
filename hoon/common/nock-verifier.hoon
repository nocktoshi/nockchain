/=  *  /common/zeke
/=  stark-verifier  /common/stark/verifier
/=  common  /common/nock-common
::
|_  stark-config
++  sam  +<
++  verifier
  =|  in=stark-input
  ::  +<+< = stark-engine door sample wrt stark-verifier core
  %_    stark-verifier
      +<+<
    %_  in
      stark-config        sam
      all-verifier-funcs  all-verifier-funcs:common
    ==
  ==
::
++  verify
  |=  [=proof override=(unit (list term)) eny=@]
  (verify:verifier proof override eny)
--
