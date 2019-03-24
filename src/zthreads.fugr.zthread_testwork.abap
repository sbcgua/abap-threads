FUNCTION ZTHREAD_TESTWORK.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     REFERENCE(I_ID) TYPE  I
*"  EXPORTING
*"     REFERENCE(E_STR)
*"----------------------------------------------------------------------

  e_str = |DOING TASK { I_ID }|.

ENDFUNCTION.
