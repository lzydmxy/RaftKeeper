; This file is generated from a similarly-named Perl script in the BoringSSL
; source tree. Do not edit by hand.

%ifdef BORINGSSL_PREFIX
%include "boringssl_prefix_symbols_nasm.inc"
%endif
%ifidn __OUTPUT_FORMAT__,obj
section	code	use32 class=code align=64
%elifidn __OUTPUT_FORMAT__,win32
%ifdef __YASM_VERSION_ID__
%if __YASM_VERSION_ID__ < 01010000h
%error yasm version 1.1.0 or later needed.
%endif
; Yasm automatically includes .00 and complains about redefining it.
; https://www.tortall.net/projects/yasm/manual/html/objfmt-win32-safeseh.html
%else
$@feat.00 equ 1
%endif
section	.text	code align=64
%else
section	.text	code
%endif
;extern	_OPENSSL_ia32cap_P
global	_bn_mul_add_words
align	16
_bn_mul_add_words:
L$_bn_mul_add_words_begin:
	lea	eax,[_OPENSSL_ia32cap_P]
	bt	DWORD [eax],26
	jnc	NEAR L$000maw_non_sse2
	mov	eax,DWORD [4+esp]
	mov	edx,DWORD [8+esp]
	mov	ecx,DWORD [12+esp]
	movd	mm0,DWORD [16+esp]
	pxor	mm1,mm1
	jmp	NEAR L$001maw_sse2_entry
align	16
L$002maw_sse2_unrolled:
	movd	mm3,DWORD [eax]
	paddq	mm1,mm3
	movd	mm2,DWORD [edx]
	pmuludq	mm2,mm0
	movd	mm4,DWORD [4+edx]
	pmuludq	mm4,mm0
	movd	mm6,DWORD [8+edx]
	pmuludq	mm6,mm0
	movd	mm7,DWORD [12+edx]
	pmuludq	mm7,mm0
	paddq	mm1,mm2
	movd	mm3,DWORD [4+eax]
	paddq	mm3,mm4
	movd	mm5,DWORD [8+eax]
	paddq	mm5,mm6
	movd	mm4,DWORD [12+eax]
	paddq	mm7,mm4
	movd	DWORD [eax],mm1
	movd	mm2,DWORD [16+edx]
	pmuludq	mm2,mm0
	psrlq	mm1,32
	movd	mm4,DWORD [20+edx]
	pmuludq	mm4,mm0
	paddq	mm1,mm3
	movd	mm6,DWORD [24+edx]
	pmuludq	mm6,mm0
	movd	DWORD [4+eax],mm1
	psrlq	mm1,32
	movd	mm3,DWORD [28+edx]
	add	edx,32
	pmuludq	mm3,mm0
	paddq	mm1,mm5
	movd	mm5,DWORD [16+eax]
	paddq	mm2,mm5
	movd	DWORD [8+eax],mm1
	psrlq	mm1,32
	paddq	mm1,mm7
	movd	mm5,DWORD [20+eax]
	paddq	mm4,mm5
	movd	DWORD [12+eax],mm1
	psrlq	mm1,32
	paddq	mm1,mm2
	movd	mm5,DWORD [24+eax]
	paddq	mm6,mm5
	movd	DWORD [16+eax],mm1
	psrlq	mm1,32
	paddq	mm1,mm4
	movd	mm5,DWORD [28+eax]
	paddq	mm3,mm5
	movd	DWORD [20+eax],mm1
	psrlq	mm1,32
	paddq	mm1,mm6
	movd	DWORD [24+eax],mm1
	psrlq	mm1,32
	paddq	mm1,mm3
	movd	DWORD [28+eax],mm1
	lea	eax,[32+eax]
	psrlq	mm1,32
	sub	ecx,8
	jz	NEAR L$003maw_sse2_exit
L$001maw_sse2_entry:
	test	ecx,4294967288
	jnz	NEAR L$002maw_sse2_unrolled
align	4
L$004maw_sse2_loop:
	movd	mm2,DWORD [edx]
	movd	mm3,DWORD [eax]
	pmuludq	mm2,mm0
	lea	edx,[4+edx]
	paddq	mm1,mm3
	paddq	mm1,mm2
	movd	DWORD [eax],mm1
	sub	ecx,1
	psrlq	mm1,32
	lea	eax,[4+eax]
	jnz	NEAR L$004maw_sse2_loop
L$003maw_sse2_exit:
	movd	eax,mm1
	emms
	ret
align	16
L$000maw_non_sse2:
	push	ebp
	push	ebx
	push	esi
	push	edi
	; 
	xor	esi,esi
	mov	edi,DWORD [20+esp]
	mov	ecx,DWORD [28+esp]
	mov	ebx,DWORD [24+esp]
	and	ecx,4294967288
	mov	ebp,DWORD [32+esp]
	push	ecx
	jz	NEAR L$005maw_finish
align	16
L$006maw_loop:
	; Round 0
	mov	eax,DWORD [ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [edi]
	adc	edx,0
	mov	DWORD [edi],eax
	mov	esi,edx
	; Round 4
	mov	eax,DWORD [4+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [4+edi]
	adc	edx,0
	mov	DWORD [4+edi],eax
	mov	esi,edx
	; Round 8
	mov	eax,DWORD [8+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [8+edi]
	adc	edx,0
	mov	DWORD [8+edi],eax
	mov	esi,edx
	; Round 12
	mov	eax,DWORD [12+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [12+edi]
	adc	edx,0
	mov	DWORD [12+edi],eax
	mov	esi,edx
	; Round 16
	mov	eax,DWORD [16+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [16+edi]
	adc	edx,0
	mov	DWORD [16+edi],eax
	mov	esi,edx
	; Round 20
	mov	eax,DWORD [20+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [20+edi]
	adc	edx,0
	mov	DWORD [20+edi],eax
	mov	esi,edx
	; Round 24
	mov	eax,DWORD [24+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [24+edi]
	adc	edx,0
	mov	DWORD [24+edi],eax
	mov	esi,edx
	; Round 28
	mov	eax,DWORD [28+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [28+edi]
	adc	edx,0
	mov	DWORD [28+edi],eax
	mov	esi,edx
	; 
	sub	ecx,8
	lea	ebx,[32+ebx]
	lea	edi,[32+edi]
	jnz	NEAR L$006maw_loop
L$005maw_finish:
	mov	ecx,DWORD [32+esp]
	and	ecx,7
	jnz	NEAR L$007maw_finish2
	jmp	NEAR L$008maw_end
L$007maw_finish2:
	; Tail Round 0
	mov	eax,DWORD [ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [edi]
	adc	edx,0
	dec	ecx
	mov	DWORD [edi],eax
	mov	esi,edx
	jz	NEAR L$008maw_end
	; Tail Round 1
	mov	eax,DWORD [4+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [4+edi]
	adc	edx,0
	dec	ecx
	mov	DWORD [4+edi],eax
	mov	esi,edx
	jz	NEAR L$008maw_end
	; Tail Round 2
	mov	eax,DWORD [8+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [8+edi]
	adc	edx,0
	dec	ecx
	mov	DWORD [8+edi],eax
	mov	esi,edx
	jz	NEAR L$008maw_end
	; Tail Round 3
	mov	eax,DWORD [12+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [12+edi]
	adc	edx,0
	dec	ecx
	mov	DWORD [12+edi],eax
	mov	esi,edx
	jz	NEAR L$008maw_end
	; Tail Round 4
	mov	eax,DWORD [16+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [16+edi]
	adc	edx,0
	dec	ecx
	mov	DWORD [16+edi],eax
	mov	esi,edx
	jz	NEAR L$008maw_end
	; Tail Round 5
	mov	eax,DWORD [20+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [20+edi]
	adc	edx,0
	dec	ecx
	mov	DWORD [20+edi],eax
	mov	esi,edx
	jz	NEAR L$008maw_end
	; Tail Round 6
	mov	eax,DWORD [24+ebx]
	mul	ebp
	add	eax,esi
	adc	edx,0
	add	eax,DWORD [24+edi]
	adc	edx,0
	mov	DWORD [24+edi],eax
	mov	esi,edx
L$008maw_end:
	mov	eax,esi
	pop	ecx
	pop	edi
	pop	esi
	pop	ebx
	pop	ebp
	ret
global	_bn_mul_words
align	16
_bn_mul_words:
L$_bn_mul_words_begin:
	lea	eax,[_OPENSSL_ia32cap_P]
	bt	DWORD [eax],26
	jnc	NEAR L$009mw_non_sse2
	mov	eax,DWORD [4+esp]
	mov	edx,DWORD [8+esp]
	mov	ecx,DWORD [12+esp]
	movd	mm0,DWORD [16+esp]
	pxor	mm1,mm1
align	16
L$010mw_sse2_loop:
	movd	mm2,DWORD [edx]
	pmuludq	mm2,mm0
	lea	edx,[4+edx]
	paddq	mm1,mm2
	movd	DWORD [eax],mm1
	sub	ecx,1
	psrlq	mm1,32
	lea	eax,[4+eax]
	jnz	NEAR L$010mw_sse2_loop
	movd	eax,mm1
	emms
	ret
align	16
L$009mw_non_sse2:
	push	ebp
	push	ebx
	push	esi
	push	edi
	; 
	xor	esi,esi
	mov	edi,DWORD [20+esp]
	mov	ebx,DWORD [24+esp]
	mov	ebp,DWORD [28+esp]
	mov	ecx,DWORD [32+esp]
	and	ebp,4294967288
	jz	NEAR L$011mw_finish
L$012mw_loop:
	; Round 0
	mov	eax,DWORD [ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [edi],eax
	mov	esi,edx
	; Round 4
	mov	eax,DWORD [4+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [4+edi],eax
	mov	esi,edx
	; Round 8
	mov	eax,DWORD [8+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [8+edi],eax
	mov	esi,edx
	; Round 12
	mov	eax,DWORD [12+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [12+edi],eax
	mov	esi,edx
	; Round 16
	mov	eax,DWORD [16+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [16+edi],eax
	mov	esi,edx
	; Round 20
	mov	eax,DWORD [20+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [20+edi],eax
	mov	esi,edx
	; Round 24
	mov	eax,DWORD [24+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [24+edi],eax
	mov	esi,edx
	; Round 28
	mov	eax,DWORD [28+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [28+edi],eax
	mov	esi,edx
	; 
	add	ebx,32
	add	edi,32
	sub	ebp,8
	jz	NEAR L$011mw_finish
	jmp	NEAR L$012mw_loop
L$011mw_finish:
	mov	ebp,DWORD [28+esp]
	and	ebp,7
	jnz	NEAR L$013mw_finish2
	jmp	NEAR L$014mw_end
L$013mw_finish2:
	; Tail Round 0
	mov	eax,DWORD [ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [edi],eax
	mov	esi,edx
	dec	ebp
	jz	NEAR L$014mw_end
	; Tail Round 1
	mov	eax,DWORD [4+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [4+edi],eax
	mov	esi,edx
	dec	ebp
	jz	NEAR L$014mw_end
	; Tail Round 2
	mov	eax,DWORD [8+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [8+edi],eax
	mov	esi,edx
	dec	ebp
	jz	NEAR L$014mw_end
	; Tail Round 3
	mov	eax,DWORD [12+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [12+edi],eax
	mov	esi,edx
	dec	ebp
	jz	NEAR L$014mw_end
	; Tail Round 4
	mov	eax,DWORD [16+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [16+edi],eax
	mov	esi,edx
	dec	ebp
	jz	NEAR L$014mw_end
	; Tail Round 5
	mov	eax,DWORD [20+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [20+edi],eax
	mov	esi,edx
	dec	ebp
	jz	NEAR L$014mw_end
	; Tail Round 6
	mov	eax,DWORD [24+ebx]
	mul	ecx
	add	eax,esi
	adc	edx,0
	mov	DWORD [24+edi],eax
	mov	esi,edx
L$014mw_end:
	mov	eax,esi
	pop	edi
	pop	esi
	pop	ebx
	pop	ebp
	ret
global	_bn_sqr_words
align	16
_bn_sqr_words:
L$_bn_sqr_words_begin:
	lea	eax,[_OPENSSL_ia32cap_P]
	bt	DWORD [eax],26
	jnc	NEAR L$015sqr_non_sse2
	mov	eax,DWORD [4+esp]
	mov	edx,DWORD [8+esp]
	mov	ecx,DWORD [12+esp]
align	16
L$016sqr_sse2_loop:
	movd	mm0,DWORD [edx]
	pmuludq	mm0,mm0
	lea	edx,[4+edx]
	movq	[eax],mm0
	sub	ecx,1
	lea	eax,[8+eax]
	jnz	NEAR L$016sqr_sse2_loop
	emms
	ret
align	16
L$015sqr_non_sse2:
	push	ebp
	push	ebx
	push	esi
	push	edi
	; 
	mov	esi,DWORD [20+esp]
	mov	edi,DWORD [24+esp]
	mov	ebx,DWORD [28+esp]
	and	ebx,4294967288
	jz	NEAR L$017sw_finish
L$018sw_loop:
	; Round 0
	mov	eax,DWORD [edi]
	mul	eax
	mov	DWORD [esi],eax
	mov	DWORD [4+esi],edx
	; Round 4
	mov	eax,DWORD [4+edi]
	mul	eax
	mov	DWORD [8+esi],eax
	mov	DWORD [12+esi],edx
	; Round 8
	mov	eax,DWORD [8+edi]
	mul	eax
	mov	DWORD [16+esi],eax
	mov	DWORD [20+esi],edx
	; Round 12
	mov	eax,DWORD [12+edi]
	mul	eax
	mov	DWORD [24+esi],eax
	mov	DWORD [28+esi],edx
	; Round 16
	mov	eax,DWORD [16+edi]
	mul	eax
	mov	DWORD [32+esi],eax
	mov	DWORD [36+esi],edx
	; Round 20
	mov	eax,DWORD [20+edi]
	mul	eax
	mov	DWORD [40+esi],eax
	mov	DWORD [44+esi],edx
	; Round 24
	mov	eax,DWORD [24+edi]
	mul	eax
	mov	DWORD [48+esi],eax
	mov	DWORD [52+esi],edx
	; Round 28
	mov	eax,DWORD [28+edi]
	mul	eax
	mov	DWORD [56+esi],eax
	mov	DWORD [60+esi],edx
	; 
	add	edi,32
	add	esi,64
	sub	ebx,8
	jnz	NEAR L$018sw_loop
L$017sw_finish:
	mov	ebx,DWORD [28+esp]
	and	ebx,7
	jz	NEAR L$019sw_end
	; Tail Round 0
	mov	eax,DWORD [edi]
	mul	eax
	mov	DWORD [esi],eax
	dec	ebx
	mov	DWORD [4+esi],edx
	jz	NEAR L$019sw_end
	; Tail Round 1
	mov	eax,DWORD [4+edi]
	mul	eax
	mov	DWORD [8+esi],eax
	dec	ebx
	mov	DWORD [12+esi],edx
	jz	NEAR L$019sw_end
	; Tail Round 2
	mov	eax,DWORD [8+edi]
	mul	eax
	mov	DWORD [16+esi],eax
	dec	ebx
	mov	DWORD [20+esi],edx
	jz	NEAR L$019sw_end
	; Tail Round 3
	mov	eax,DWORD [12+edi]
	mul	eax
	mov	DWORD [24+esi],eax
	dec	ebx
	mov	DWORD [28+esi],edx
	jz	NEAR L$019sw_end
	; Tail Round 4
	mov	eax,DWORD [16+edi]
	mul	eax
	mov	DWORD [32+esi],eax
	dec	ebx
	mov	DWORD [36+esi],edx
	jz	NEAR L$019sw_end
	; Tail Round 5
	mov	eax,DWORD [20+edi]
	mul	eax
	mov	DWORD [40+esi],eax
	dec	ebx
	mov	DWORD [44+esi],edx
	jz	NEAR L$019sw_end
	; Tail Round 6
	mov	eax,DWORD [24+edi]
	mul	eax
	mov	DWORD [48+esi],eax
	mov	DWORD [52+esi],edx
L$019sw_end:
	pop	edi
	pop	esi
	pop	ebx
	pop	ebp
	ret
global	_bn_div_words
align	16
_bn_div_words:
L$_bn_div_words_begin:
	mov	edx,DWORD [4+esp]
	mov	eax,DWORD [8+esp]
	mov	ecx,DWORD [12+esp]
	div	ecx
	ret
global	_bn_add_words
align	16
_bn_add_words:
L$_bn_add_words_begin:
	push	ebp
	push	ebx
	push	esi
	push	edi
	; 
	mov	ebx,DWORD [20+esp]
	mov	esi,DWORD [24+esp]
	mov	edi,DWORD [28+esp]
	mov	ebp,DWORD [32+esp]
	xor	eax,eax
	and	ebp,4294967288
	jz	NEAR L$020aw_finish
L$021aw_loop:
	; Round 0
	mov	ecx,DWORD [esi]
	mov	edx,DWORD [edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [ebx],ecx
	; Round 1
	mov	ecx,DWORD [4+esi]
	mov	edx,DWORD [4+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [4+ebx],ecx
	; Round 2
	mov	ecx,DWORD [8+esi]
	mov	edx,DWORD [8+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [8+ebx],ecx
	; Round 3
	mov	ecx,DWORD [12+esi]
	mov	edx,DWORD [12+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [12+ebx],ecx
	; Round 4
	mov	ecx,DWORD [16+esi]
	mov	edx,DWORD [16+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [16+ebx],ecx
	; Round 5
	mov	ecx,DWORD [20+esi]
	mov	edx,DWORD [20+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [20+ebx],ecx
	; Round 6
	mov	ecx,DWORD [24+esi]
	mov	edx,DWORD [24+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [24+ebx],ecx
	; Round 7
	mov	ecx,DWORD [28+esi]
	mov	edx,DWORD [28+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [28+ebx],ecx
	; 
	add	esi,32
	add	edi,32
	add	ebx,32
	sub	ebp,8
	jnz	NEAR L$021aw_loop
L$020aw_finish:
	mov	ebp,DWORD [32+esp]
	and	ebp,7
	jz	NEAR L$022aw_end
	; Tail Round 0
	mov	ecx,DWORD [esi]
	mov	edx,DWORD [edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [ebx],ecx
	jz	NEAR L$022aw_end
	; Tail Round 1
	mov	ecx,DWORD [4+esi]
	mov	edx,DWORD [4+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [4+ebx],ecx
	jz	NEAR L$022aw_end
	; Tail Round 2
	mov	ecx,DWORD [8+esi]
	mov	edx,DWORD [8+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [8+ebx],ecx
	jz	NEAR L$022aw_end
	; Tail Round 3
	mov	ecx,DWORD [12+esi]
	mov	edx,DWORD [12+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [12+ebx],ecx
	jz	NEAR L$022aw_end
	; Tail Round 4
	mov	ecx,DWORD [16+esi]
	mov	edx,DWORD [16+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [16+ebx],ecx
	jz	NEAR L$022aw_end
	; Tail Round 5
	mov	ecx,DWORD [20+esi]
	mov	edx,DWORD [20+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [20+ebx],ecx
	jz	NEAR L$022aw_end
	; Tail Round 6
	mov	ecx,DWORD [24+esi]
	mov	edx,DWORD [24+edi]
	add	ecx,eax
	mov	eax,0
	adc	eax,eax
	add	ecx,edx
	adc	eax,0
	mov	DWORD [24+ebx],ecx
L$022aw_end:
	pop	edi
	pop	esi
	pop	ebx
	pop	ebp
	ret
global	_bn_sub_words
align	16
_bn_sub_words:
L$_bn_sub_words_begin:
	push	ebp
	push	ebx
	push	esi
	push	edi
	; 
	mov	ebx,DWORD [20+esp]
	mov	esi,DWORD [24+esp]
	mov	edi,DWORD [28+esp]
	mov	ebp,DWORD [32+esp]
	xor	eax,eax
	and	ebp,4294967288
	jz	NEAR L$023aw_finish
L$024aw_loop:
	; Round 0
	mov	ecx,DWORD [esi]
	mov	edx,DWORD [edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [ebx],ecx
	; Round 1
	mov	ecx,DWORD [4+esi]
	mov	edx,DWORD [4+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [4+ebx],ecx
	; Round 2
	mov	ecx,DWORD [8+esi]
	mov	edx,DWORD [8+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [8+ebx],ecx
	; Round 3
	mov	ecx,DWORD [12+esi]
	mov	edx,DWORD [12+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [12+ebx],ecx
	; Round 4
	mov	ecx,DWORD [16+esi]
	mov	edx,DWORD [16+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [16+ebx],ecx
	; Round 5
	mov	ecx,DWORD [20+esi]
	mov	edx,DWORD [20+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [20+ebx],ecx
	; Round 6
	mov	ecx,DWORD [24+esi]
	mov	edx,DWORD [24+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [24+ebx],ecx
	; Round 7
	mov	ecx,DWORD [28+esi]
	mov	edx,DWORD [28+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [28+ebx],ecx
	; 
	add	esi,32
	add	edi,32
	add	ebx,32
	sub	ebp,8
	jnz	NEAR L$024aw_loop
L$023aw_finish:
	mov	ebp,DWORD [32+esp]
	and	ebp,7
	jz	NEAR L$025aw_end
	; Tail Round 0
	mov	ecx,DWORD [esi]
	mov	edx,DWORD [edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [ebx],ecx
	jz	NEAR L$025aw_end
	; Tail Round 1
	mov	ecx,DWORD [4+esi]
	mov	edx,DWORD [4+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [4+ebx],ecx
	jz	NEAR L$025aw_end
	; Tail Round 2
	mov	ecx,DWORD [8+esi]
	mov	edx,DWORD [8+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [8+ebx],ecx
	jz	NEAR L$025aw_end
	; Tail Round 3
	mov	ecx,DWORD [12+esi]
	mov	edx,DWORD [12+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [12+ebx],ecx
	jz	NEAR L$025aw_end
	; Tail Round 4
	mov	ecx,DWORD [16+esi]
	mov	edx,DWORD [16+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [16+ebx],ecx
	jz	NEAR L$025aw_end
	; Tail Round 5
	mov	ecx,DWORD [20+esi]
	mov	edx,DWORD [20+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	dec	ebp
	mov	DWORD [20+ebx],ecx
	jz	NEAR L$025aw_end
	; Tail Round 6
	mov	ecx,DWORD [24+esi]
	mov	edx,DWORD [24+edi]
	sub	ecx,eax
	mov	eax,0
	adc	eax,eax
	sub	ecx,edx
	adc	eax,0
	mov	DWORD [24+ebx],ecx
L$025aw_end:
	pop	edi
	pop	esi
	pop	ebx
	pop	ebp
	ret
segment	.bss
common	_OPENSSL_ia32cap_P 16
