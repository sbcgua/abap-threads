FUNCTION ZTHREAD_TESTWORK.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(I_ID) TYPE  I
*"  EXPORTING
*"     VALUE(E_STR) TYPE  STRING
*"----------------------------------------------------------------------

  e_str = |DOING TASK { I_ID }|.

ENDFUNCTION.
