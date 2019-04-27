FUNCTION Z_THREAD_RUNNER .
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IV_RUNNER_CLASS_NAME) TYPE  STRING
*"     VALUE(IV_RAW_PARAMS) TYPE  XSTRING
*"  EXPORTING
*"     VALUE(EV_RAW_RESULT) TYPE  XSTRING
*"  EXCEPTIONS
*"      ERROR
*"----------------------------------------------------------------------

  data lo_runner type ref to object.
  data lx type ref to cx_root.
  data lv_text type c length 200.

  try.
    create object lo_runner type (iv_runner_class_name).
    call method lo_runner->('DESERIALIZE_STATE') exporting iv_xstr = iv_raw_params.
    call method lo_runner->('RUN').
    call method lo_runner->('SERIALIZE_STATE') receiving rv_xstr = ev_raw_result.
  catch cx_root into lx.
    lv_text = lx->get_text( ).
    message s000(oo) raising error with
      lv_text+0(50)
      lv_text+50(50)
      lv_text+100(50)
      lv_text+150(50).
  endtry.

ENDFUNCTION.
