	.file	"q4.cpp"
	.text
	.section	.text._ZNKSt5ctypeIcE8do_widenEc,"axG",@progbits,_ZNKSt5ctypeIcE8do_widenEc,comdat
	.align 2
	.p2align 4
	.weak	_ZNKSt5ctypeIcE8do_widenEc
	.type	_ZNKSt5ctypeIcE8do_widenEc, @function
_ZNKSt5ctypeIcE8do_widenEc:
.LFB1558:
	.cfi_startproc
	endbr64
	movl	%esi, %eax
	ret
	.cfi_endproc
.LFE1558:
	.size	_ZNKSt5ctypeIcE8do_widenEc, .-_ZNKSt5ctypeIcE8do_widenEc
	.text
	.p2align 4
	.type	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3, @function
_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3:
.LFB5186:
	.cfi_startproc
	endbr64
	pushq	%r14
	.cfi_def_cfa_offset 16
	.cfi_offset 14, -16
	pushq	%r13
	.cfi_def_cfa_offset 24
	.cfi_offset 13, -24
	pushq	%r12
	.cfi_def_cfa_offset 32
	.cfi_offset 12, -32
	movq	%rdi, %r12
	pushq	%rbp
	.cfi_def_cfa_offset 40
	.cfi_offset 6, -40
	pushq	%rbx
	.cfi_def_cfa_offset 48
	.cfi_offset 3, -48
	call	omp_get_num_threads@PLT
	movl	%eax, %ebx
	call	omp_get_thread_num@PLT
	movslq	%eax, %rsi
	movq	24(%r12), %rax
	movslq	%ebx, %rcx
	cqto
	idivq	%rcx
	cmpq	%rdx, %rsi
	jl	.L4
.L11:
	movq	%rax, %rcx
	imulq	%rsi, %rcx
	addq	%rcx, %rdx
	addq	%rdx, %rax
	cmpq	%rax, %rdx
	jge	.L15
	leaq	(%rsi,%rsi,4), %rbx
	movq	16(%r12), %rbp
	movq	8(%r12), %r8
	movq	(%r12), %r11
	movq	32(%r12), %r9
	salq	$3, %rbx
	movq	40(%r12), %r12
	movabsq	$-7046029254386353131, %r10
	jmp	.L7
	.p2align 4,,10
	.p2align 3
.L8:
	incq	%rdx
	cmpq	%rdx, %rax
	je	.L15
.L7:
	movl	(%r8,%rdx,4), %edi
	leal	-8582(%rdi), %ecx
	cmpl	$91, %ecx
	ja	.L8
	movslq	(%r11,%rdx,4), %rcx
	movq	24(%r9), %rdi
	movq	%rcx, %rsi
	imulq	%r10, %rcx
	movq	(%r9), %r14
	andq	%rdi, %rcx
	leaq	(%r14,%rcx,8), %r13
	cmpb	$0, 4(%r13)
	jne	.L10
	jmp	.L8
	.p2align 4,,10
	.p2align 3
.L17:
	incq	%rcx
	andq	%rdi, %rcx
	leaq	(%r14,%rcx,8), %r13
	cmpb	$0, 4(%r13)
	je	.L8
.L10:
	cmpl	0(%r13), %esi
	jne	.L17
	movslq	0(%rbp,%rdx,4), %rcx
	cmpl	$4, %ecx
	ja	.L8
	movq	(%r12), %rsi
	incq	%rdx
	addq	%rbx, %rsi
	incq	(%rsi,%rcx,8)
	cmpq	%rdx, %rax
	jne	.L7
.L15:
	popq	%rbx
	.cfi_remember_state
	.cfi_def_cfa_offset 40
	popq	%rbp
	.cfi_def_cfa_offset 32
	popq	%r12
	.cfi_def_cfa_offset 24
	popq	%r13
	.cfi_def_cfa_offset 16
	popq	%r14
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L4:
	.cfi_restore_state
	incq	%rax
	xorl	%edx, %edx
	jmp	.L11
	.cfi_endproc
.LFE5186:
	.size	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3, .-_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3
	.p2align 4
	.type	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.0, @function
_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.0:
.LFB5183:
	.cfi_startproc
	endbr64
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	pushq	%r13
	pushq	%r12
	pushq	%rbx
	.cfi_offset 13, -24
	.cfi_offset 12, -32
	.cfi_offset 3, -40
	movq	%rdi, %rbx
	andq	$-32, %rsp
	movq	(%rdi), %r13
	call	omp_get_num_threads@PLT
	movl	%eax, %r12d
	call	omp_get_thread_num@PLT
	movslq	%eax, %rcx
	movq	8(%rbx), %rax
	movslq	%r12d, %rsi
	cqto
	idivq	%rsi
	cmpq	%rdx, %rcx
	jl	.L19
.L32:
	imulq	%rax, %rcx
	addq	%rcx, %rdx
	leaq	(%rax,%rdx), %rdi
	xorl	%ecx, %ecx
	cmpq	%rdi, %rdx
	jge	.L20
	leaq	-1(%rax), %rcx
	cmpq	$6, %rcx
	jbe	.L34
	movq	%rax, %rsi
	shrq	$3, %rsi
	leaq	0(%r13,%rdx,4), %rcx
	salq	$5, %rsi
	vpbroadcastd	.LC3(%rip), %ymm5
	vpbroadcastd	.LC4(%rip), %ymm4
	vpbroadcastd	.LC5(%rip), %ymm3
	addq	%rcx, %rsi
	vpxor	%xmm1, %xmm1, %xmm1
	.p2align 4,,10
	.p2align 3
.L22:
	vpaddd	(%rcx), %ymm5, %ymm0
	addq	$32, %rcx
	vpminud	%ymm4, %ymm0, %ymm2
	vpcmpeqd	%ymm2, %ymm0, %ymm0
	vpand	%ymm3, %ymm0, %ymm0
	vpmovsxdq	%xmm0, %ymm2
	vextracti128	$0x1, %ymm0, %xmm0
	vpaddq	%ymm1, %ymm2, %ymm1
	vpmovsxdq	%xmm0, %ymm0
	vpaddq	%ymm1, %ymm0, %ymm1
	cmpq	%rsi, %rcx
	jne	.L22
	vmovdqa	%xmm1, %xmm0
	vextracti64x2	$0x1, %ymm1, %xmm1
	vpaddq	%xmm1, %xmm0, %xmm0
	vpsrldq	$8, %xmm0, %xmm1
	movq	%rax, %rsi
	vpaddq	%xmm1, %xmm0, %xmm0
	andq	$-8, %rsi
	vmovq	%xmm0, %rcx
	addq	%rsi, %rdx
	cmpq	%rsi, %rax
	je	.L40
	vzeroupper
.L21:
	movl	0(%r13,%rdx,4), %esi
	leaq	0(,%rdx,4), %rax
	subl	$8582, %esi
	cmpl	$92, %esi
	leaq	1(%rdx), %rsi
	adcq	$0, %rcx
	cmpq	%rdi, %rsi
	jge	.L20
	movl	4(%r13,%rax), %esi
	subl	$8582, %esi
	cmpl	$92, %esi
	leaq	2(%rdx), %rsi
	adcq	$0, %rcx
	cmpq	%rsi, %rdi
	jle	.L20
	movl	8(%r13,%rax), %esi
	subl	$8582, %esi
	cmpl	$92, %esi
	leaq	3(%rdx), %rsi
	adcq	$0, %rcx
	cmpq	%rsi, %rdi
	jle	.L20
	movl	12(%r13,%rax), %esi
	subl	$8582, %esi
	cmpl	$92, %esi
	leaq	4(%rdx), %rsi
	adcq	$0, %rcx
	cmpq	%rsi, %rdi
	jle	.L20
	movl	16(%r13,%rax), %esi
	subl	$8582, %esi
	cmpl	$92, %esi
	leaq	5(%rdx), %rsi
	adcq	$0, %rcx
	cmpq	%rsi, %rdi
	jle	.L20
	movl	20(%r13,%rax), %esi
	subl	$8582, %esi
	cmpl	$92, %esi
	adcq	$0, %rcx
	addq	$6, %rdx
	cmpq	%rdx, %rdi
	jle	.L20
	movl	24(%r13,%rax), %eax
	subl	$8582, %eax
	cmpl	$92, %eax
	adcq	$0, %rcx
.L20:
	lock addq	%rcx, 16(%rbx)
	leaq	-24(%rbp), %rsp
	popq	%rbx
	popq	%r12
	popq	%r13
	popq	%rbp
	.cfi_remember_state
	.cfi_def_cfa 7, 8
	ret
	.p2align 4,,10
	.p2align 3
.L19:
	.cfi_restore_state
	incq	%rax
	xorl	%edx, %edx
	jmp	.L32
	.p2align 4,,10
	.p2align 3
.L34:
	xorl	%ecx, %ecx
	jmp	.L21
	.p2align 4,,10
	.p2align 3
.L40:
	vzeroupper
	jmp	.L20
	.cfi_endproc
.LFE5183:
	.size	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.0, .-_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.0
	.p2align 4
	.type	_ZSt25__unguarded_linear_insertIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops14_Val_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_.constprop.0, @function
_ZSt25__unguarded_linear_insertIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops14_Val_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_.constprop.0:
.LFB5197:
	.cfi_startproc
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	leaq	16(%rdi), %rbp
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$72, %rsp
	.cfi_def_cfa_offset 128
	movq	(%rdi), %r12
	movq	%fs:40, %rax
	movq	%rax, 56(%rsp)
	xorl	%eax, %eax
	leaq	32(%rsp), %rax
	movq	%rax, 8(%rsp)
	movq	%rax, 16(%rsp)
	cmpq	%rbp, %r12
	je	.L81
	movq	16(%rdi), %rax
	movq	%r12, 16(%rsp)
	movq	%rax, 32(%rsp)
.L43:
	movq	8(%rdi), %r13
	movq	32(%rdi), %rax
	movq	%r13, 24(%rsp)
	movq	%rbp, (%rdi)
	movq	$0, 8(%rdi)
	movb	$0, 16(%rdi)
	movq	%rax, 48(%rsp)
	jmp	.L44
	.p2align 4,,10
	.p2align 3
.L45:
	cmpq	%rbp, %rdi
	je	.L82
	movq	(%rbx), %rdx
	movq	40(%rbx), %rax
	movq	%rcx, 24(%rbx)
	movq	%r15, 32(%rbx)
	movq	%rdx, 40(%rbx)
	testq	%rdi, %rdi
	je	.L50
	movq	%rdi, -16(%rbx)
	movq	%rax, (%rbx)
.L46:
	movq	$0, -8(%rbx)
	movb	$0, (%rdi)
	movq	%rbx, %rbp
	movq	16(%rbx), %rax
	movq	16(%rsp), %r12
	movq	%rax, 56(%rbx)
	movq	24(%rsp), %r13
.L44:
	movq	-48(%rbp), %r15
	movq	%r13, %rdx
	cmpq	%r13, %r15
	cmovbe	%r15, %rdx
	movq	-56(%rbp), %rcx
	leaq	-16(%rbp), %r14
	testq	%rdx, %rdx
	je	.L51
	movq	%rcx, %rsi
	movq	%r12, %rdi
	movq	%rcx, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %rcx
	jne	.L52
.L51:
	movq	%r13, %rax
	subq	%r15, %rax
	movl	$2147483648, %esi
	cmpq	%rsi, %rax
	jge	.L53
	movabsq	$-2147483649, %rsi
	cmpq	%rsi, %rax
	jle	.L54
.L52:
	testl	%eax, %eax
	jns	.L53
.L54:
	leaq	-40(%rbp), %rbx
	movq	-16(%rbp), %rdi
	cmpq	%rbx, %rcx
	jne	.L45
	subq	$56, %rbp
	cmpq	%rbp, %r14
	je	.L63
	testq	%r15, %r15
	je	.L47
	cmpq	$1, %r15
	je	.L83
	movq	%r15, %rdx
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	-8(%rbx), %r15
	movq	24(%rbx), %rdi
.L47:
	movq	%r15, 32(%rbx)
	movb	$0, (%rdi,%r15)
	movq	-16(%rbx), %rdi
	jmp	.L46
	.p2align 4,,10
	.p2align 3
.L82:
	movq	-8(%rbx), %rax
	movq	%rcx, 24(%rbx)
	movq	%rax, 32(%rbx)
	movq	(%rbx), %rax
	movq	%rax, 40(%rbx)
.L50:
	movq	%rbx, -16(%rbx)
	movq	%rbx, %rdi
	jmp	.L46
	.p2align 4,,10
	.p2align 3
.L53:
	movq	(%r14), %rdi
	cmpq	8(%rsp), %r12
	je	.L84
	movq	32(%rsp), %rax
	vmovq	%r13, %xmm1
	vpinsrq	$1, %rax, %xmm1, %xmm0
	cmpq	%rbp, %rdi
	je	.L85
	movq	16(%r14), %rdx
	movq	%r12, (%r14)
	movq	%r13, 8(%r14)
	movq	%rax, 16(%r14)
	testq	%rdi, %rdi
	je	.L60
	movq	%rdi, 16(%rsp)
	movq	%rdx, 32(%rsp)
.L58:
	movq	$0, 24(%rsp)
	movb	$0, (%rdi)
	movq	48(%rsp), %rax
	movq	16(%rsp), %rdi
	movq	%rax, 32(%r14)
	cmpq	8(%rsp), %rdi
	je	.L41
	movq	32(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L41:
	movq	56(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L86
	addq	$72, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L85:
	.cfi_restore_state
	movq	%r12, (%r14)
	vmovdqu	%xmm0, 8(%r14)
.L60:
	movq	8(%rsp), %rax
	movq	%rax, 16(%rsp)
	leaq	32(%rsp), %rax
	movq	%rax, 8(%rsp)
	movq	%rax, %rdi
	jmp	.L58
	.p2align 4,,10
	.p2align 3
.L83:
	movzbl	(%rbx), %eax
	movb	%al, (%rdi)
	movq	-8(%rbx), %r15
	movq	24(%rbx), %rdi
	jmp	.L47
	.p2align 4,,10
	.p2align 3
.L81:
	vmovdqu	16(%rdi), %xmm2
	movq	%rax, %r12
	vmovdqa	%xmm2, 32(%rsp)
	jmp	.L43
	.p2align 4,,10
	.p2align 3
.L84:
	testq	%r13, %r13
	je	.L56
	cmpq	$1, %r13
	je	.L87
	movq	8(%rsp), %rsi
	movq	%r13, %rdx
	call	memcpy@PLT
	movq	24(%rsp), %r13
	movq	(%r14), %rdi
.L56:
	movq	%r13, 8(%r14)
	movb	$0, (%rdi,%r13)
	movq	16(%rsp), %rdi
	jmp	.L58
	.p2align 4,,10
	.p2align 3
.L63:
	movq	%rbx, %rdi
	jmp	.L46
.L87:
	movzbl	32(%rsp), %eax
	movb	%al, (%rdi)
	movq	24(%rsp), %r13
	movq	(%r14), %rdi
	jmp	.L56
.L86:
	call	__stack_chk_fail@PLT
	.cfi_endproc
.LFE5197:
	.size	_ZSt25__unguarded_linear_insertIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops14_Val_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_.constprop.0, .-_ZSt25__unguarded_linear_insertIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops14_Val_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_.constprop.0
	.p2align 4
	.type	_ZSt16__insertion_sortIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_.constprop.0, @function
_ZSt16__insertion_sortIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_.constprop.0:
.LFB5200:
	.cfi_startproc
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$72, %rsp
	.cfi_def_cfa_offset 128
	movq	%rsi, 8(%rsp)
	movq	%fs:40, %rax
	movq	%rax, 56(%rsp)
	xorl	%eax, %eax
	cmpq	%rsi, %rdi
	je	.L88
	leaq	40(%rdi), %rax
	movq	%rdi, %r14
	cmpq	%rax, %rsi
	je	.L88
	leaq	56(%rdi), %r15
	leaq	32(%rsp), %rbp
	.p2align 4,,10
	.p2align 3
.L113:
	movq	-8(%r15), %rbx
	movq	8(%r14), %r13
	movq	-16(%r15), %rdi
	cmpq	%r13, %rbx
	movq	%r13, %rdx
	cmovbe	%rbx, %rdx
	movq	(%r14), %rsi
	leaq	-16(%r15), %r12
	testq	%rdx, %rdx
	je	.L91
	movq	%rdi, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %rdi
	jne	.L92
.L91:
	movq	%rbx, %rax
	subq	%r13, %rax
	movl	$2147483648, %ecx
	cmpq	%rcx, %rax
	jge	.L93
	movabsq	$-2147483649, %rsi
	cmpq	%rsi, %rax
	jle	.L94
.L92:
	testl	%eax, %eax
	jns	.L93
.L94:
	movq	%rbp, 16(%rsp)
	cmpq	%r15, %rdi
	je	.L134
	movq	(%r15), %rax
	movq	%rdi, 16(%rsp)
	movq	%rax, 32(%rsp)
.L96:
	movq	%r12, %r11
	subq	%r14, %r11
	movq	16(%r15), %rax
	movq	%r11, %rcx
	sarq	$3, %rcx
	movabsq	$-3689348814741910323, %r12
	movq	%rbx, 24(%rsp)
	movq	%r15, -16(%r15)
	movq	$0, -8(%r15)
	movb	$0, (%r15)
	movq	%rax, 48(%rsp)
	imulq	%rcx, %r12
	leaq	24(%r15), %r13
	testq	%r11, %r11
	jle	.L97
	leaq	-40(%r15), %rbx
	jmp	.L104
	.p2align 4,,10
	.p2align 3
.L98:
	leaq	40(%rbx), %rdx
	cmpq	%rdx, %rdi
	je	.L135
	movq	%rax, 24(%rbx)
	movq	-8(%rbx), %rax
	movq	40(%rbx), %rdx
	movq	%rax, 32(%rbx)
	movq	(%rbx), %rax
	movq	%rax, 40(%rbx)
	testq	%rdi, %rdi
	je	.L103
	movq	%rdi, -16(%rbx)
	movq	%rdx, (%rbx)
	movq	%rdi, %rax
.L99:
	movq	$0, -8(%rbx)
	movb	$0, (%rax)
	subq	$40, %rbx
	movq	56(%rbx), %rax
	movq	%rax, 96(%rbx)
	decq	%r12
	je	.L136
.L104:
	movq	-16(%rbx), %rax
	movq	24(%rbx), %rdi
	cmpq	%rbx, %rax
	jne	.L98
	movq	-8(%rbx), %rdx
	testq	%rdx, %rdx
	je	.L100
	cmpq	$1, %rdx
	je	.L137
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	24(%rbx), %rdi
	movq	-8(%rbx), %rdx
.L100:
	movq	%rdx, 32(%rbx)
	movb	$0, (%rdi,%rdx)
	movq	-16(%rbx), %rax
	jmp	.L99
	.p2align 4,,10
	.p2align 3
.L135:
	movq	%rax, 24(%rbx)
	movq	-8(%rbx), %rax
	movq	%rax, 32(%rbx)
	movq	(%rbx), %rax
	movq	%rax, 40(%rbx)
.L103:
	movq	%rbx, -16(%rbx)
	movq	%rbx, %rax
	jmp	.L99
	.p2align 4,,10
	.p2align 3
.L136:
	movq	16(%rsp), %rdi
	movq	24(%rsp), %rbx
.L97:
	movq	(%r14), %r11
	cmpq	%rbp, %rdi
	je	.L138
	movq	32(%rsp), %rax
	vmovq	%rbx, %xmm1
	leaq	16(%r14), %rdx
	vpinsrq	$1, %rax, %xmm1, %xmm0
	cmpq	%rdx, %r11
	je	.L139
	movq	16(%r14), %rdx
	movq	%rdi, (%r14)
	movq	%rbx, 8(%r14)
	movq	%rax, 16(%r14)
	testq	%r11, %r11
	je	.L110
	movq	%r11, 16(%rsp)
	movq	%rdx, 32(%rsp)
.L108:
	movq	$0, 24(%rsp)
	movb	$0, (%r11)
	movq	48(%rsp), %rax
	movq	16(%rsp), %rdi
	movq	%rax, 32(%r14)
	cmpq	%rbp, %rdi
	je	.L112
	movq	32(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L112:
	addq	$40, %r15
	cmpq	%r13, 8(%rsp)
	jne	.L113
.L88:
	movq	56(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L140
	addq	$72, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L93:
	.cfi_restore_state
	movq	%r12, %rdi
	call	_ZSt25__unguarded_linear_insertIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops14_Val_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_.constprop.0
	leaq	24(%r15), %r13
	jmp	.L112
	.p2align 4,,10
	.p2align 3
.L137:
	movzbl	(%rbx), %eax
	movb	%al, (%rdi)
	movq	24(%rbx), %rdi
	movq	-8(%rbx), %rdx
	jmp	.L100
	.p2align 4,,10
	.p2align 3
.L134:
	vmovdqu	(%rdi), %xmm2
	movq	%rbp, %rdi
	vmovdqa	%xmm2, 32(%rsp)
	jmp	.L96
	.p2align 4,,10
	.p2align 3
.L138:
	testq	%rbx, %rbx
	je	.L106
	cmpq	$1, %rbx
	je	.L141
	movq	%rbx, %rdx
	movq	%r11, %rdi
	movq	%rbp, %rsi
	call	memcpy@PLT
	movq	24(%rsp), %rbx
	movq	(%r14), %r11
.L106:
	movq	%rbx, 8(%r14)
	movb	$0, (%r11,%rbx)
	movq	16(%rsp), %r11
	jmp	.L108
	.p2align 4,,10
	.p2align 3
.L139:
	movq	%rdi, (%r14)
	vmovdqu	%xmm0, 8(%r14)
.L110:
	movq	%rbp, 16(%rsp)
	movq	%rbp, %r11
	jmp	.L108
.L141:
	movzbl	32(%rsp), %eax
	movb	%al, (%r11)
	movq	24(%rsp), %rbx
	movq	(%r14), %r11
	movq	%rbx, 8(%r14)
	movb	$0, (%r11,%rbx)
	movq	16(%rsp), %r11
	jmp	.L108
.L140:
	call	__stack_chk_fail@PLT
	.cfi_endproc
.LFE5200:
	.size	_ZSt16__insertion_sortIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_.constprop.0, .-_ZSt16__insertion_sortIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_.constprop.0
	.p2align 4
	.type	_ZSt13__adjust_heapIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElS9_NS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_SM_T1_T2_.constprop.0, @function
_ZSt13__adjust_heapIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElS9_NS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_SM_T1_T2_.constprop.0:
.LFB5201:
	.cfi_startproc
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	movq	%rdi, %r14
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$136, %rsp
	.cfi_def_cfa_offset 192
	movq	%rdx, 72(%rsp)
	movq	%rcx, 64(%rsp)
	movq	%rsi, 48(%rsp)
	decq	%rdx
	movq	%fs:40, %rax
	movq	%rax, 120(%rsp)
	xorl	%eax, %eax
	movq	%rdx, %rax
	shrq	$63, %rax
	addq	%rdx, %rax
	sarq	%rax
	movq	%rax, 56(%rsp)
	movq	%rax, %rcx
	leaq	(%rsi,%rsi,4), %rax
	leaq	(%rdi,%rax,8), %rbx
	leaq	16(%rbx), %r10
	cmpq	%rcx, %rsi
	jge	.L184
	movq	%rdi, %r11
	movq	%rsi, %r14
	jmp	.L153
	.p2align 4,,10
	.p2align 3
.L147:
	cmpq	%r10, %rdi
	je	.L223
	movq	%r15, 0(%r13)
	movq	%r12, 8(%r13)
	movq	16(%r13), %rax
	movq	16(%rbx), %rdx
	movq	%rdx, 16(%r13)
	testq	%rdi, %rdi
	je	.L152
	movq	%rdi, (%rbx)
	movq	%rax, 16(%rbx)
.L148:
	movq	$0, 8(%rbx)
	movb	$0, (%rdi)
	movq	32(%rbx), %rax
	movq	%rax, 32(%r13)
	cmpq	56(%rsp), %rbp
	jge	.L224
	movq	%r14, %r10
	movq	%rbp, %r14
.L153:
	leaq	1(%r14), %rax
	leaq	(%rax,%rax), %rcx
	leaq	-1(%rcx), %rbp
	leaq	0(%rbp,%rbp,4), %rdx
	leaq	(%rcx,%rax,8), %rax
	leaq	(%r11,%rdx,8), %rbx
	leaq	(%r11,%rax,8), %r8
	movq	8(%r8), %r13
	movq	8(%rbx), %r12
	movq	(%r8), %rdi
	cmpq	%r12, %r13
	movq	%r12, %rdx
	cmovbe	%r13, %rdx
	movq	(%rbx), %r15
	testq	%rdx, %rdx
	je	.L144
	movq	%r15, %rsi
	movq	%r11, 40(%rsp)
	movq	%r10, 32(%rsp)
	movq	%r8, 24(%rsp)
	movq	%rcx, 16(%rsp)
	movq	%rdi, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %rdi
	movq	16(%rsp), %rcx
	movq	24(%rsp), %r8
	movq	32(%rsp), %r10
	movq	40(%rsp), %r11
	jne	.L145
.L144:
	movq	%r13, %rax
	subq	%r12, %rax
	movl	$2147483648, %esi
	cmpq	%rsi, %rax
	jge	.L185
	movabsq	$-2147483649, %rsi
	cmpq	%rsi, %rax
	jle	.L146
.L145:
	testl	%eax, %eax
	cmovns	%r13, %r12
	cmovns	%rdi, %r15
	cmovns	%r8, %rbx
	cmovns	%rcx, %rbp
.L146:
	leaq	(%r14,%r14,4), %rax
	leaq	(%r11,%rax,8), %r13
	leaq	16(%rbx), %r14
	movq	0(%r13), %rdi
	cmpq	%r15, %r14
	jne	.L147
	cmpq	%rbx, %r13
	je	.L186
	testq	%r12, %r12
	je	.L149
	cmpq	$1, %r12
	je	.L225
	movq	%r12, %rdx
	movq	%r15, %rsi
	movq	%r11, (%rsp)
	call	memcpy@PLT
	movq	8(%rbx), %r12
	movq	0(%r13), %rdi
	movq	(%rsp), %r11
.L149:
	movq	%r12, 8(%r13)
	movb	$0, (%rdi,%r12)
	movq	(%rbx), %rdi
	jmp	.L148
	.p2align 4,,10
	.p2align 3
.L223:
	movq	%r15, 0(%r13)
	movq	%r12, 8(%r13)
	movq	16(%rbx), %rax
	movq	%rax, 16(%r13)
.L152:
	movq	%r14, (%rbx)
	movq	%r14, %rdi
	jmp	.L148
	.p2align 4,,10
	.p2align 3
.L185:
	movq	%r13, %r12
	movq	%rdi, %r15
	movq	%r8, %rbx
	movq	%rcx, %rbp
	jmp	.L146
	.p2align 4,,10
	.p2align 3
.L224:
	movq	%r14, %r13
	movq	%r11, %r14
.L143:
	movq	72(%rsp), %rax
	testb	$1, %al
	jne	.L154
	subq	$2, %rax
	movq	%rax, %rdx
	shrq	$63, %rdx
	addq	%rdx, %rax
	sarq	%rax
	cmpq	%rbp, %rax
	je	.L226
.L154:
	leaq	96(%rsp), %rax
	movq	%rax, 32(%rsp)
	movq	%rax, 80(%rsp)
	movq	64(%rsp), %rax
	movq	(%rax), %r12
	addq	$16, %rax
	cmpq	%rax, %r12
	je	.L227
	movq	64(%rsp), %rsi
	movq	%r12, 80(%rsp)
	movq	16(%rsi), %rdx
	movq	%rsi, %rcx
	movq	%rdx, 96(%rsp)
.L162:
	movq	%rax, (%rcx)
	movq	32(%rcx), %rax
	movq	8(%rcx), %r15
	movq	%rax, 112(%rsp)
	leaq	-1(%rbp), %rax
	movq	%rax, %r9
	shrq	$63, %r9
	addq	%rax, %r9
	movq	%r15, 88(%rsp)
	movq	$0, 8(%rcx)
	movb	$0, 16(%rcx)
	sarq	%r9
	cmpq	48(%rsp), %rbp
	jle	.L163
	movq	%r12, %rsi
	movq	%r14, %r8
	movq	%r9, %r12
	movq	%rbp, %r14
	movq	%r13, %r9
	jmp	.L175
	.p2align 4,,10
	.p2align 3
.L168:
	cmpq	%r9, %rdi
	je	.L228
	movq	%rbp, (%r15)
	movq	%r13, 8(%r15)
	movq	16(%r15), %rax
	movq	16(%rbx), %rdx
	movq	%rdx, 16(%r15)
	testq	%rdi, %rdi
	je	.L173
	movq	%rdi, (%rbx)
	movq	%rax, 16(%rbx)
.L169:
	movq	$0, 8(%rbx)
	movb	$0, (%rdi)
	leaq	-1(%r12), %rdx
	movq	32(%rbx), %rax
	movq	%rax, 32(%r15)
	movq	%rdx, %rax
	shrq	$63, %rax
	addq	%rdx, %rax
	sarq	%rax
	cmpq	%r12, 48(%rsp)
	jge	.L229
	movq	80(%rsp), %rsi
	movq	88(%rsp), %r15
	movq	%r14, %r9
	movq	%r12, %r14
	movq	%rax, %r12
.L175:
	leaq	(%r12,%r12,4), %rax
	leaq	(%r8,%rax,8), %rbx
	movq	8(%rbx), %r13
	movq	%r15, %rdx
	cmpq	%r15, %r13
	cmovbe	%r13, %rdx
	movq	(%rbx), %rbp
	testq	%rdx, %rdx
	je	.L164
	movq	%rbp, %rdi
	movq	%r8, 24(%rsp)
	movq	%r9, 16(%rsp)
	movq	%rsi, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %rsi
	movq	16(%rsp), %r9
	movq	24(%rsp), %r8
	jne	.L165
.L164:
	movq	%r13, %rax
	subq	%r15, %rax
	movl	$2147483648, %ecx
	cmpq	%rcx, %rax
	jge	.L222
	movabsq	$-2147483649, %rcx
	cmpq	%rcx, %rax
	jle	.L167
.L165:
	testl	%eax, %eax
	jns	.L222
.L167:
	leaq	(%r14,%r14,4), %rax
	leaq	(%r8,%rax,8), %r15
	leaq	16(%rbx), %r14
	movq	(%r15), %rdi
	cmpq	%r14, %rbp
	jne	.L168
	cmpq	%r15, %rbx
	je	.L189
	testq	%r13, %r13
	je	.L170
	cmpq	$1, %r13
	je	.L230
	movq	%r13, %rdx
	movq	%r14, %rsi
	movq	%r8, (%rsp)
	call	memcpy@PLT
	movq	8(%rbx), %r13
	movq	(%r15), %rdi
	movq	(%rsp), %r8
.L170:
	movq	%r13, 8(%r15)
	movb	$0, (%rdi,%r13)
	movq	(%rbx), %rdi
	jmp	.L169
	.p2align 4,,10
	.p2align 3
.L228:
	movq	%rbp, (%r15)
	movq	%r13, 8(%r15)
	movq	16(%rbx), %rax
	movq	%rax, 16(%r15)
.L173:
	movq	%r14, (%rbx)
	movq	%r14, %rdi
	jmp	.L169
	.p2align 4,,10
	.p2align 3
.L222:
	leaq	(%r14,%r14,4), %rax
	movq	%rsi, %r12
	movq	%r9, %r13
	leaq	(%r8,%rax,8), %rbx
.L163:
	movq	(%rbx), %rdi
	cmpq	32(%rsp), %r12
	je	.L231
.L176:
	movq	96(%rsp), %rax
	vmovq	%r15, %xmm1
	vpinsrq	$1, %rax, %xmm1, %xmm0
	cmpq	%r13, %rdi
	je	.L232
	movq	16(%rbx), %rdx
	movq	%r12, (%rbx)
	movq	%r15, 8(%rbx)
	movq	%rax, 16(%rbx)
	testq	%rdi, %rdi
	je	.L181
	movq	%rdi, 80(%rsp)
	movq	%rdx, 96(%rsp)
.L179:
	movq	$0, 88(%rsp)
	movb	$0, (%rdi)
	movq	112(%rsp), %rax
	movq	80(%rsp), %rdi
	movq	%rax, 32(%rbx)
	cmpq	32(%rsp), %rdi
	je	.L142
	movq	96(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L142:
	movq	120(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L233
	addq	$136, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L232:
	.cfi_restore_state
	movq	%r12, (%rbx)
	vmovdqu	%xmm0, 8(%rbx)
.L181:
	movq	32(%rsp), %rax
	movq	%rax, 80(%rsp)
	leaq	96(%rsp), %rax
	movq	%rax, 32(%rsp)
	movq	%rax, %rdi
	jmp	.L179
	.p2align 4,,10
	.p2align 3
.L229:
	movq	80(%rsp), %r12
	movq	88(%rsp), %r15
	movq	(%rbx), %rdi
	movq	%r14, %r13
	cmpq	32(%rsp), %r12
	jne	.L176
.L231:
	testq	%r15, %r15
	je	.L177
	cmpq	$1, %r15
	je	.L234
	movq	32(%rsp), %rsi
	movq	%r15, %rdx
	call	memcpy@PLT
	movq	88(%rsp), %r15
	movq	(%rbx), %rdi
.L177:
	movq	%r15, 8(%rbx)
	movb	$0, (%rdi,%r15)
	movq	80(%rsp), %rdi
	jmp	.L179
	.p2align 4,,10
	.p2align 3
.L230:
	movzbl	16(%rbx), %eax
	movb	%al, (%rdi)
	movq	8(%rbx), %r13
	movq	(%r15), %rdi
	jmp	.L170
	.p2align 4,,10
	.p2align 3
.L225:
	movzbl	16(%rbx), %eax
	movb	%al, (%rdi)
	movq	8(%rbx), %r12
	movq	0(%r13), %rdi
	jmp	.L149
	.p2align 4,,10
	.p2align 3
.L227:
	movq	64(%rsp), %rsi
	movq	32(%rsp), %r12
	vmovdqu	16(%rsi), %xmm2
	movq	%rsi, %rcx
	vmovdqa	%xmm2, (%rsp)
	vmovdqa	%xmm2, 96(%rsp)
	jmp	.L162
	.p2align 4,,10
	.p2align 3
.L189:
	movq	%r14, %rdi
	jmp	.L169
	.p2align 4,,10
	.p2align 3
.L186:
	movq	%r15, %rdi
	jmp	.L148
	.p2align 4,,10
	.p2align 3
.L226:
	leaq	1(%rbp,%rbp), %rbp
	leaq	0(%rbp,%rbp,4), %rax
	leaq	(%r14,%rax,8), %r12
	movq	(%r12), %rax
	leaq	16(%r12), %r15
	movq	(%rbx), %rdi
	cmpq	%r15, %rax
	je	.L235
	movq	8(%r12), %rdx
	cmpq	%r13, %rdi
	je	.L236
	movq	%rax, (%rbx)
	movq	%rdx, 8(%rbx)
	movq	16(%rbx), %rcx
	movq	16(%r12), %rax
	movq	%rax, 16(%rbx)
	testq	%rdi, %rdi
	je	.L160
	movq	%rdi, (%r12)
	movq	%rcx, 16(%r12)
.L156:
	movq	$0, 8(%r12)
	movb	$0, (%rdi)
	movq	%r15, %r13
	movq	32(%r12), %rax
	movq	%rax, 32(%rbx)
	movq	%r12, %rbx
	jmp	.L154
	.p2align 4,,10
	.p2align 3
.L184:
	movq	%rsi, %rbp
	movq	%r10, %r13
	jmp	.L143
.L236:
	movq	%rax, (%rbx)
	movq	%rdx, 8(%rbx)
	movq	16(%r12), %rax
	movq	%rax, 16(%rbx)
.L160:
	movq	%r15, (%r12)
	movq	%r15, %rdi
	jmp	.L156
.L234:
	movzbl	96(%rsp), %eax
	movb	%al, (%rdi)
	movq	88(%rsp), %r15
	movq	(%rbx), %rdi
	jmp	.L177
.L235:
	cmpq	%rbx, %r12
	je	.L188
	movq	8(%r12), %rdx
	testq	%rdx, %rdx
	je	.L157
	cmpq	$1, %rdx
	je	.L237
	movq	%r15, %rsi
	call	memcpy@PLT
	movq	8(%r12), %rdx
	movq	(%rbx), %rdi
.L157:
	movq	%rdx, 8(%rbx)
	movb	$0, (%rdi,%rdx)
	movq	(%r12), %rdi
	jmp	.L156
.L237:
	movzbl	16(%r12), %eax
	movb	%al, (%rdi)
	movq	8(%r12), %rdx
	movq	(%rbx), %rdi
	jmp	.L157
.L188:
	movq	%r15, %rdi
	jmp	.L156
.L233:
	call	__stack_chk_fail@PLT
	.cfi_endproc
.LFE5201:
	.size	_ZSt13__adjust_heapIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElS9_NS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_SM_T1_T2_.constprop.0, .-_ZSt13__adjust_heapIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElS9_NS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_SM_T1_T2_.constprop.0
	.p2align 4
	.type	_ZSt16__introsort_loopIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElNS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_T1_, @function
_ZSt16__introsort_loopIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElNS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_T1_:
.LFB4815:
	.cfi_startproc
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$152, %rsp
	.cfi_def_cfa_offset 208
	movq	%rdi, 8(%rsp)
	movq	%rdx, 24(%rsp)
	movq	%fs:40, %rax
	movq	%rax, 136(%rsp)
	movq	%rsi, %rax
	movq	%rsi, 16(%rsp)
	subq	%rdi, %rax
	cmpq	$640, %rax
	jle	.L238
	cmpq	$0, 24(%rsp)
	je	.L335
	movq	%rdi, %rax
	addq	$40, %rax
	movq	%rax, 32(%rsp)
	movq	16(%rsp), %rdx
.L242:
	movq	8(%rsp), %rdi
	movabsq	$-3689348814741910323, %rax
	subq	%rdi, %rdx
	sarq	$3, %rdx
	imulq	%rax, %rdx
	movq	48(%rdi), %rbx
	decq	24(%rsp)
	movq	%rdx, %rax
	shrq	$63, %rax
	addq	%rdx, %rax
	sarq	%rax
	leaq	(%rax,%rax,4), %rax
	leaq	(%rdi,%rax,8), %r12
	movq	8(%r12), %rbp
	movq	40(%rdi), %r9
	cmpq	%rbp, %rbx
	movq	%rbp, %rdx
	cmovbe	%rbx, %rdx
	movq	(%r12), %r8
	testq	%rdx, %rdx
	je	.L266
	movq	%r8, %rsi
	movq	%r9, %rdi
	movq	%r8, 40(%rsp)
	movq	%r9, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %r9
	movq	40(%rsp), %r8
	jne	.L267
.L266:
	movq	%rbx, %rax
	subq	%rbp, %rax
	movl	$2147483648, %edi
	cmpq	%rdi, %rax
	jge	.L268
	movabsq	$-2147483649, %rdi
	cmpq	%rdi, %rax
	jle	.L269
.L267:
	testl	%eax, %eax
	jns	.L268
.L269:
	movq	16(%rsp), %rax
	movq	-32(%rax), %r15
	movq	-40(%rax), %rsi
	cmpq	%r15, %rbp
	movq	%r15, %rdx
	cmovbe	%rbp, %rdx
	testq	%rdx, %rdx
	je	.L270
	movq	%r8, %rdi
	movq	%r9, 40(%rsp)
	movq	%rsi, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %rsi
	movq	40(%rsp), %r9
	jne	.L271
.L270:
	movq	%rbp, %rax
	subq	%r15, %rax
	movl	$2147483648, %edi
	cmpq	%rdi, %rax
	jge	.L272
	movabsq	$-2147483649, %rdi
	cmpq	%rdi, %rax
	jle	.L285
.L271:
	testl	%eax, %eax
	js	.L285
.L272:
	cmpq	%r15, %rbx
	movq	%r15, %rdx
	cmovbe	%rbx, %rdx
	testq	%rdx, %rdx
	je	.L275
	movq	%r9, %rdi
	call	memcmp@PLT
	testl	%eax, %eax
	je	.L275
.L276:
	testl	%eax, %eax
	js	.L286
.L282:
	movq	8(%rsp), %rbx
	movq	32(%rsp), %rsi
	movq	%rbx, %rdi
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE4swapERS4_@PLT
	movq	32(%rbx), %rax
	movq	72(%rbx), %rdx
	movq	%rax, 72(%rbx)
	movq	%rdx, 32(%rbx)
.L274:
	movq	16(%rsp), %rax
	movq	32(%rsp), %r15
	movq	%rax, (%rsp)
	movq	%r15, %rbp
	movq	%r15, %r13
	.p2align 4,,10
	.p2align 3
.L299:
	movq	8(%rsp), %rax
	movq	(%rax), %rbx
	movq	8(%rax), %r15
	jmp	.L287
	.p2align 4,,10
	.p2align 3
.L336:
	movabsq	$-2147483649, %rax
	cmpq	%rax, %r12
	jle	.L291
	movl	%r12d, %eax
.L289:
	testl	%eax, %eax
	jns	.L290
.L291:
	leaq	40(%r13), %rbp
	movq	%rbp, %r13
.L287:
	movq	8(%r13), %r12
	movq	%r15, %rdx
	cmpq	%r15, %r12
	cmovbe	%r12, %rdx
	testq	%rdx, %rdx
	je	.L288
	movq	0(%r13), %rdi
	movq	%rbx, %rsi
	call	memcmp@PLT
	testl	%eax, %eax
	jne	.L289
.L288:
	subq	%r15, %r12
	movl	$2147483648, %eax
	cmpq	%rax, %r12
	jl	.L336
.L290:
	movq	(%rsp), %r14
	movq	%rbp, (%rsp)
	subq	$40, %r14
	movq	%r15, %rbp
	jmp	.L292
	.p2align 4,,10
	.p2align 3
.L337:
	movabsq	$-2147483649, %rcx
	cmpq	%rcx, %rax
	jle	.L296
.L294:
	testl	%eax, %eax
	jns	.L295
.L296:
	subq	$40, %r14
.L292:
	movq	8(%r14), %r15
	movq	(%r14), %rsi
	cmpq	%r15, %rbp
	movq	%r15, %rdx
	cmovbe	%rbp, %rdx
	movq	%r14, %r12
	testq	%rdx, %rdx
	je	.L293
	movq	%rbx, %rdi
	call	memcmp@PLT
	testl	%eax, %eax
	jne	.L294
.L293:
	movq	%rbp, %rax
	subq	%r15, %rax
	movl	$2147483648, %ecx
	cmpq	%rcx, %rax
	jl	.L337
.L295:
	movq	(%rsp), %rbp
	movq	%r12, (%rsp)
	cmpq	%r14, %r13
	jnb	.L338
	movq	%rbp, %rdi
	movq	%r14, %rsi
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE4swapERS4_@PLT
	movq	32(%r13), %rax
	movq	32(%r14), %rdx
	leaq	40(%r13), %rbp
	movq	%rdx, 32(%r13)
	movq	%rax, 32(%r14)
	movq	%rbp, %r13
	jmp	.L299
.L275:
	movq	%rbx, %rax
	subq	%r15, %rax
	movl	$2147483648, %edi
	cmpq	%rdi, %rax
	jge	.L282
	movabsq	$-2147483649, %rdi
	cmpq	%rdi, %rax
	jg	.L276
	jmp	.L286
	.p2align 4,,10
	.p2align 3
.L338:
	movq	24(%rsp), %rdx
	movq	16(%rsp), %rsi
	movq	%r13, %rdi
	call	_ZSt16__introsort_loopIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElNS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_T1_
	movq	%r13, %rax
	subq	8(%rsp), %rax
	movq	%r13, %r15
	cmpq	$640, %rax
	jle	.L238
	cmpq	$0, 24(%rsp)
	je	.L241
	movq	%r15, 16(%rsp)
	movq	%r15, %rdx
	jmp	.L242
.L268:
	movq	16(%rsp), %rax
	movq	-32(%rax), %r15
	movq	-40(%rax), %rsi
	cmpq	%r15, %rbx
	movq	%r15, %rdx
	cmovbe	%rbx, %rdx
	testq	%rdx, %rdx
	je	.L279
	movq	%r9, %rdi
	movq	%r8, 40(%rsp)
	movq	%rsi, (%rsp)
	call	memcmp@PLT
	testl	%eax, %eax
	movq	(%rsp), %rsi
	movq	40(%rsp), %r8
	jne	.L280
.L279:
	subq	%r15, %rbx
	movl	$2147483648, %eax
	cmpq	%rax, %rbx
	jge	.L281
	movabsq	$-2147483649, %rax
	cmpq	%rax, %rbx
	jle	.L282
	movl	%ebx, %eax
.L280:
	testl	%eax, %eax
	js	.L282
.L281:
	cmpq	%r15, %rbp
	movq	%r15, %rdx
	cmovbe	%rbp, %rdx
	testq	%rdx, %rdx
	je	.L283
	movq	%r8, %rdi
	call	memcmp@PLT
	testl	%eax, %eax
	jne	.L284
.L283:
	subq	%r15, %rbp
	movl	$2147483648, %eax
	cmpq	%rax, %rbp
	jge	.L285
	movabsq	$-2147483649, %rax
	cmpq	%rax, %rbp
	jle	.L286
	movl	%ebp, %eax
.L284:
	testl	%eax, %eax
	jns	.L285
.L286:
	movq	16(%rsp), %rbx
	movq	8(%rsp), %r14
	leaq	-40(%rbx), %rsi
	movq	%r14, %rdi
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE4swapERS4_@PLT
	movq	32(%r14), %rax
	movq	-8(%rbx), %rdx
	movq	%rdx, 32(%r14)
	movq	%rax, -8(%rbx)
	jmp	.L274
.L285:
	movq	8(%rsp), %rbx
	movq	%r12, %rsi
	movq	%rbx, %rdi
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE4swapERS4_@PLT
	movq	32(%rbx), %rax
	movq	32(%r12), %rdx
	movq	%rdx, 32(%rbx)
	movq	%rax, 32(%r12)
	jmp	.L274
.L343:
	movq	(%rsp), %r15
	cmpq	%rbx, %rdi
	je	.L249
	movq	64(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L249:
	movq	8(%rsp), %rax
	movq	%r14, (%rsp)
	leaq	16(%rax), %r13
	subq	$24, %r15
	movq	%rax, %r14
	.p2align 4,,10
	.p2align 3
.L250:
	movq	-16(%r15), %rax
	movq	%rbx, 48(%rsp)
	leaq	-16(%r15), %rbp
	cmpq	%r15, %rax
	je	.L339
	movq	%rax, 48(%rsp)
	movq	(%r15), %rax
	movq	%rax, 64(%rsp)
.L254:
	movq	-8(%r15), %rax
	movq	%r15, -16(%r15)
	movb	$0, (%r15)
	movq	%rax, 56(%rsp)
	movq	16(%r15), %rax
	movq	$0, -8(%r15)
	movq	%rax, 80(%rsp)
	movq	(%r14), %rax
	cmpq	%r13, %rax
	je	.L340
	movq	%rax, -16(%r15)
	movq	8(%r14), %rax
	movq	%rax, -8(%r15)
	movq	16(%r14), %rax
	movq	%rax, (%r15)
	movq	%r13, (%r14)
	movq	%r13, %rax
.L256:
	movq	$0, 8(%r14)
	movb	$0, (%rax)
	movq	%r12, 96(%rsp)
	movq	32(%r14), %rax
	movq	%rax, 16(%r15)
	movq	48(%rsp), %rax
	cmpq	%rbx, %rax
	je	.L341
	movq	%rax, 96(%rsp)
	movq	64(%rsp), %rax
	movq	%rax, 112(%rsp)
.L260:
	movq	56(%rsp), %rax
	subq	%r14, %rbp
	movq	%rax, 104(%rsp)
	movq	80(%rsp), %rax
	movq	%rbp, %rdx
	movq	%rax, 128(%rsp)
	sarq	$3, %rdx
	movabsq	$-3689348814741910323, %rax
	imulq	%rax, %rdx
	movq	(%rsp), %rcx
	movq	%r14, %rdi
	xorl	%esi, %esi
	movq	%rbx, 48(%rsp)
	movq	$0, 56(%rsp)
	movb	$0, 64(%rsp)
	call	_ZSt13__adjust_heapIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElS9_NS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_SM_T1_T2_.constprop.0
	movq	96(%rsp), %rdi
	cmpq	%r12, %rdi
	je	.L261
	movq	112(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L261:
	movq	48(%rsp), %rdi
	cmpq	%rbx, %rdi
	je	.L262
	movq	64(%rsp), %rax
	subq	$40, %r15
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	cmpq	$40, %rbp
	jg	.L250
.L238:
	movq	136(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L342
	addq	$152, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
.L335:
	.cfi_restore_state
	movq	%rsi, %r15
.L241:
	movabsq	$-3689348814741910323, %rdx
	sarq	$3, %rax
	imulq	%rdx, %rax
	movq	%r15, (%rsp)
	leaq	112(%rsp), %r12
	movq	%rax, %rdi
	leaq	-2(%rax), %rax
	sarq	%rax
	leaq	(%rax,%rax,4), %rdx
	movq	%rax, %rbp
	movq	8(%rsp), %rax
	leaq	64(%rsp), %rbx
	leaq	16(%rax,%rdx,8), %r13
	movq	%r13, %r15
	leaq	96(%rsp), %r14
	movq	%rdi, %r13
	jmp	.L252
.L243:
	movq	(%r15), %rdi
	movq	-8(%r15), %rsi
	movq	16(%r15), %rcx
	movq	%rdx, 48(%rsp)
	movq	%rdi, 64(%rsp)
	movq	%rsi, 56(%rsp)
	movq	%r15, -16(%r15)
	movq	$0, -8(%r15)
	movb	$0, (%r15)
	movq	%rcx, 80(%rsp)
	movq	%r12, 96(%rsp)
	cmpq	%rbx, %rdx
	je	.L244
	movq	%rdx, 96(%rsp)
	movq	%rdi, 112(%rsp)
.L246:
	movq	8(%rsp), %rdi
	movq	%rsi, 104(%rsp)
	movq	%rcx, 128(%rsp)
	movq	%r13, %rdx
	movq	%r14, %rcx
	movq	%rbp, %rsi
	movq	%rbx, 48(%rsp)
	movq	$0, 56(%rsp)
	movb	$0, 64(%rsp)
	call	_ZSt13__adjust_heapIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElS9_NS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_SM_T1_T2_.constprop.0
	movq	96(%rsp), %rdi
	cmpq	%r12, %rdi
	je	.L247
	movq	112(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L247:
	movq	48(%rsp), %rdi
	testq	%rbp, %rbp
	je	.L343
	decq	%rbp
	cmpq	%rbx, %rdi
	je	.L251
	movq	64(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L251:
	subq	$40, %r15
.L252:
	movq	-16(%r15), %rdx
	cmpq	%r15, %rdx
	jne	.L243
	movq	16(%r15), %rcx
	vmovdqu	(%r15), %xmm2
	movq	-8(%r15), %rsi
	movb	$0, (%r15)
	movq	$0, -8(%r15)
	movq	%rcx, 80(%rsp)
	movq	%r12, 96(%rsp)
	vmovdqa	%xmm2, 64(%rsp)
.L244:
	vmovdqa	64(%rsp), %xmm3
	vmovdqa	%xmm3, 112(%rsp)
	jmp	.L246
.L262:
	subq	$40, %r15
	cmpq	$40, %rbp
	jg	.L250
	jmp	.L238
.L341:
	vmovdqa	64(%rsp), %xmm1
	vmovdqa	%xmm1, 112(%rsp)
	jmp	.L260
.L340:
	cmpq	%rbp, %r14
	je	.L302
	movq	8(%r14), %rdx
	testq	%rdx, %rdx
	je	.L257
	cmpq	$1, %rdx
	je	.L344
	movq	%r13, %rsi
	movq	%r15, %rdi
	call	memcpy@PLT
	movq	8(%r14), %rdx
.L257:
	movq	-16(%r15), %rax
	movq	%rdx, -8(%r15)
	movb	$0, (%rax,%rdx)
	movq	(%r14), %rax
	jmp	.L256
.L339:
	vmovdqu	(%r15), %xmm0
	vmovdqa	%xmm0, 64(%rsp)
	jmp	.L254
.L344:
	movzbl	16(%r14), %eax
	movb	%al, (%r15)
	movq	8(%r14), %rdx
	jmp	.L257
.L302:
	movq	%r13, %rax
	jmp	.L256
.L342:
	call	__stack_chk_fail@PLT
	.cfi_endproc
.LFE4815:
	.size	_ZSt16__introsort_loopIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElNS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_T1_, .-_ZSt16__introsort_loopIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElNS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_T1_
	.section	.rodata._ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE.str1.1,"aMS",@progbits,1
.LC6:
	.string	"Failed to open: "
.LC7:
	.string	"fstat failed for: "
.LC8:
	.string	"mmap failed for: "
	.section	.text._ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE,"axG",@progbits,_ZN8MmapFileC5ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE,comdat
	.align 2
	.p2align 4
	.weak	_ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.type	_ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE, @function
_ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE:
.LFB3904:
	.cfi_startproc
	endbr64
	pushq	%r12
	.cfi_def_cfa_offset 16
	.cfi_offset 12, -16
	pushq	%rbp
	.cfi_def_cfa_offset 24
	.cfi_offset 6, -24
	movq	%rsi, %rbp
	xorl	%esi, %esi
	pushq	%rbx
	.cfi_def_cfa_offset 32
	.cfi_offset 3, -32
	movq	%rdi, %rbx
	subq	$160, %rsp
	.cfi_def_cfa_offset 192
	movq	%fs:40, %rax
	movq	%rax, 152(%rsp)
	xorl	%eax, %eax
	movq	$0, (%rdi)
	movq	$0, 8(%rdi)
	movl	$-1, 16(%rdi)
	movq	0(%rbp), %rdi
	call	open@PLT
	movl	%eax, 16(%rbx)
	testl	%eax, %eax
	js	.L371
	movl	%eax, %edi
	movq	%rsp, %rsi
	call	fstat@PLT
	testl	%eax, %eax
	js	.L372
	movq	48(%rsp), %rsi
	movl	16(%rbx), %r8d
	movq	%rsi, 8(%rbx)
	xorl	%r9d, %r9d
	movl	$1, %ecx
	movl	$1, %edx
	xorl	%edi, %edi
	call	mmap@PLT
	movq	%rax, (%rbx)
	cmpq	$-1, %rax
	je	.L373
.L345:
	movq	152(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L370
	addq	$160, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 32
	popq	%rbx
	.cfi_def_cfa_offset 24
	popq	%rbp
	.cfi_def_cfa_offset 16
	popq	%r12
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L371:
	.cfi_restore_state
	leaq	_ZSt4cerr(%rip), %r12
	movq	%r12, %rdi
	movl	$16, %edx
	leaq	.LC6(%rip), %rsi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	8(%rbp), %rdx
	movq	0(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %rbp
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%rbp,%rax), %r12
	testq	%r12, %r12
	je	.L352
	cmpb	$0, 56(%r12)
	je	.L348
	movsbl	67(%r12), %esi
.L349:
	movq	%rbp, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	movq	152(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L370
	addq	$160, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 32
	popq	%rbx
	.cfi_def_cfa_offset 24
	popq	%rbp
	.cfi_def_cfa_offset 16
	popq	%r12
	.cfi_def_cfa_offset 8
	jmp	_ZNSo5flushEv@PLT
	.p2align 4,,10
	.p2align 3
.L348:
	.cfi_restore_state
	movq	%r12, %rdi
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L349
	movq	%r12, %rdi
	call	*%rax
	movsbl	%al, %esi
	jmp	.L349
	.p2align 4,,10
	.p2align 3
.L373:
	leaq	_ZSt4cerr(%rip), %r12
	movq	%r12, %rdi
	movl	$17, %edx
	leaq	.LC8(%rip), %rsi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	8(%rbp), %rdx
	movq	0(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %rbp
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%rbp,%rax), %r12
	testq	%r12, %r12
	je	.L352
	cmpb	$0, 56(%r12)
	je	.L356
	movsbl	67(%r12), %esi
.L357:
	movq	%rbp, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	16(%rbx), %edi
	call	close@PLT
	movl	$-1, 16(%rbx)
	movq	$0, (%rbx)
	movq	$0, 8(%rbx)
	jmp	.L345
	.p2align 4,,10
	.p2align 3
.L372:
	leaq	_ZSt4cerr(%rip), %r12
	movq	%r12, %rdi
	movl	$18, %edx
	leaq	.LC7(%rip), %rsi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	8(%rbp), %rdx
	movq	0(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %rbp
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%rbp,%rax), %r12
	testq	%r12, %r12
	je	.L352
	cmpb	$0, 56(%r12)
	je	.L353
	movsbl	67(%r12), %esi
.L354:
	movq	%rbp, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	16(%rbx), %edi
	call	close@PLT
	movl	$-1, 16(%rbx)
	jmp	.L345
	.p2align 4,,10
	.p2align 3
.L353:
	movq	%r12, %rdi
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L354
	movq	%r12, %rdi
	call	*%rax
	movsbl	%al, %esi
	jmp	.L354
	.p2align 4,,10
	.p2align 3
.L356:
	movq	%r12, %rdi
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L357
	movq	%r12, %rdi
	call	*%rax
	movsbl	%al, %esi
	jmp	.L357
.L370:
	call	__stack_chk_fail@PLT
.L352:
	call	_ZSt16__throw_bad_castv@PLT
	.cfi_endproc
.LFE3904:
	.size	_ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE, .-_ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.weak	_ZN8MmapFileC1ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.set	_ZN8MmapFileC1ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE,_ZN8MmapFileC2ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.section	.text._ZN8MmapFileD2Ev,"axG",@progbits,_ZN8MmapFileD5Ev,comdat
	.align 2
	.p2align 4
	.weak	_ZN8MmapFileD2Ev
	.type	_ZN8MmapFileD2Ev, @function
_ZN8MmapFileD2Ev:
.LFB3907:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA3907
	endbr64
	pushq	%rbx
	.cfi_def_cfa_offset 16
	.cfi_offset 3, -16
	movq	%rdi, %rbx
	movq	(%rdi), %rdi
	leaq	-1(%rdi), %rax
	cmpq	$-3, %rax
	jbe	.L379
.L375:
	movl	16(%rbx), %edi
	testl	%edi, %edi
	jns	.L380
	popq	%rbx
	.cfi_remember_state
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L380:
	.cfi_restore_state
	call	close@PLT
	popq	%rbx
	.cfi_remember_state
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L379:
	.cfi_restore_state
	movq	8(%rbx), %rsi
	call	munmap@PLT
	jmp	.L375
	.cfi_endproc
.LFE3907:
	.globl	__gxx_personality_v0
	.section	.gcc_except_table._ZN8MmapFileD2Ev,"aG",@progbits,_ZN8MmapFileD5Ev,comdat
.LLSDA3907:
	.byte	0xff
	.byte	0xff
	.byte	0x1
	.uleb128 .LLSDACSE3907-.LLSDACSB3907
.LLSDACSB3907:
.LLSDACSE3907:
	.section	.text._ZN8MmapFileD2Ev,"axG",@progbits,_ZN8MmapFileD5Ev,comdat
	.size	_ZN8MmapFileD2Ev, .-_ZN8MmapFileD2Ev
	.weak	_ZN8MmapFileD1Ev
	.set	_ZN8MmapFileD1Ev,_ZN8MmapFileD2Ev
	.section	.text._ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev,"axG",@progbits,_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED5Ev,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev
	.type	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev, @function
_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev:
.LFB4284:
	.cfi_startproc
	endbr64
	pushq	%r12
	.cfi_def_cfa_offset 16
	.cfi_offset 12, -16
	movq	%rdi, %r12
	pushq	%rbp
	.cfi_def_cfa_offset 24
	.cfi_offset 6, -24
	pushq	%rbx
	.cfi_def_cfa_offset 32
	.cfi_offset 3, -32
	movq	8(%rdi), %rbx
	movq	(%rdi), %rbp
	cmpq	%rbp, %rbx
	je	.L382
	.p2align 4,,10
	.p2align 3
.L386:
	movq	0(%rbp), %rdi
	leaq	16(%rbp), %rax
	cmpq	%rax, %rdi
	je	.L383
	movq	16(%rbp), %rax
	addq	$32, %rbp
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	cmpq	%rbp, %rbx
	jne	.L386
.L385:
	movq	(%r12), %rbp
.L382:
	testq	%rbp, %rbp
	je	.L388
	movq	16(%r12), %rsi
	popq	%rbx
	.cfi_remember_state
	.cfi_def_cfa_offset 24
	subq	%rbp, %rsi
	movq	%rbp, %rdi
	popq	%rbp
	.cfi_def_cfa_offset 16
	popq	%r12
	.cfi_def_cfa_offset 8
	jmp	_ZdlPvm@PLT
	.p2align 4,,10
	.p2align 3
.L383:
	.cfi_restore_state
	addq	$32, %rbp
	cmpq	%rbp, %rbx
	jne	.L386
	jmp	.L385
	.p2align 4,,10
	.p2align 3
.L388:
	popq	%rbx
	.cfi_def_cfa_offset 24
	popq	%rbp
	.cfi_def_cfa_offset 16
	popq	%r12
	.cfi_def_cfa_offset 8
	ret
	.cfi_endproc
.LFE4284:
	.size	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev, .-_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev
	.weak	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED1Ev
	.set	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED1Ev,_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED2Ev
	.section	.text._ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev,"axG",@progbits,_ZNSt6vectorIS_IiSaIiEESaIS1_EED5Ev,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev
	.type	_ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev, @function
_ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev:
.LFB4353:
	.cfi_startproc
	endbr64
	pushq	%r12
	.cfi_def_cfa_offset 16
	.cfi_offset 12, -16
	movq	%rdi, %r12
	pushq	%rbp
	.cfi_def_cfa_offset 24
	.cfi_offset 6, -24
	pushq	%rbx
	.cfi_def_cfa_offset 32
	.cfi_offset 3, -32
	movq	8(%rdi), %rbx
	movq	(%rdi), %rbp
	cmpq	%rbp, %rbx
	je	.L391
	.p2align 4,,10
	.p2align 3
.L395:
	movq	0(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L392
	movq	16(%rbp), %rsi
	addq	$24, %rbp
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
	cmpq	%rbp, %rbx
	jne	.L395
.L394:
	movq	(%r12), %rbp
.L391:
	testq	%rbp, %rbp
	je	.L397
	movq	16(%r12), %rsi
	popq	%rbx
	.cfi_remember_state
	.cfi_def_cfa_offset 24
	subq	%rbp, %rsi
	movq	%rbp, %rdi
	popq	%rbp
	.cfi_def_cfa_offset 16
	popq	%r12
	.cfi_def_cfa_offset 8
	jmp	_ZdlPvm@PLT
	.p2align 4,,10
	.p2align 3
.L392:
	.cfi_restore_state
	addq	$24, %rbp
	cmpq	%rbp, %rbx
	jne	.L395
	jmp	.L394
	.p2align 4,,10
	.p2align 3
.L397:
	popq	%rbx
	.cfi_def_cfa_offset 24
	popq	%rbp
	.cfi_def_cfa_offset 16
	popq	%r12
	.cfi_def_cfa_offset 8
	ret
	.cfi_endproc
.LFE4353:
	.size	_ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev, .-_ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev
	.weak	_ZNSt6vectorIS_IiSaIiEESaIS1_EED1Ev
	.set	_ZNSt6vectorIS_IiSaIiEESaIS1_EED1Ev,_ZNSt6vectorIS_IiSaIiEESaIS1_EED2Ev
	.section	.rodata._ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_.str1.8,"aMS",@progbits,1
	.align 8
.LC9:
	.string	"basic_string::_M_construct null not valid"
	.section	.text._ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_,"axG",@progbits,_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC5IS3_EEPKcRKS3_,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_
	.type	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_, @function
_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_:
.LFB4426:
	.cfi_startproc
	endbr64
	pushq	%r13
	.cfi_def_cfa_offset 16
	.cfi_offset 13, -16
	pushq	%r12
	.cfi_def_cfa_offset 24
	.cfi_offset 12, -24
	leaq	16(%rdi), %r12
	pushq	%rbp
	.cfi_def_cfa_offset 32
	.cfi_offset 6, -32
	pushq	%rbx
	.cfi_def_cfa_offset 40
	.cfi_offset 3, -40
	subq	$24, %rsp
	.cfi_def_cfa_offset 64
	movq	%fs:40, %rax
	movq	%rax, 8(%rsp)
	xorl	%eax, %eax
	movq	%r12, (%rdi)
	testq	%rsi, %rsi
	je	.L400
	movq	%rdi, %rbx
	movq	%rsi, %rdi
	movq	%rsi, %rbp
	call	strlen@PLT
	movq	%rax, (%rsp)
	movq	%rax, %r13
	cmpq	$15, %rax
	ja	.L413
	cmpq	$1, %rax
	jne	.L404
	movzbl	0(%rbp), %edx
	movb	%dl, 16(%rbx)
.L405:
	movq	%rax, 8(%rbx)
	movb	$0, (%r12,%rax)
	movq	8(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L414
	addq	$24, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 40
	popq	%rbx
	.cfi_def_cfa_offset 32
	popq	%rbp
	.cfi_def_cfa_offset 24
	popq	%r12
	.cfi_def_cfa_offset 16
	popq	%r13
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L404:
	.cfi_restore_state
	testq	%rax, %rax
	je	.L405
	jmp	.L403
	.p2align 4,,10
	.p2align 3
.L413:
	movq	%rsp, %rsi
	xorl	%edx, %edx
	movq	%rbx, %rdi
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
	movq	%rax, (%rbx)
	movq	%rax, %r12
	movq	(%rsp), %rax
	movq	%rax, 16(%rbx)
.L403:
	movq	%r12, %rdi
	movq	%r13, %rdx
	movq	%rbp, %rsi
	call	memcpy@PLT
	movq	(%rsp), %rax
	movq	(%rbx), %r12
	jmp	.L405
.L414:
	call	__stack_chk_fail@PLT
.L400:
	leaq	.LC9(%rip), %rdi
	call	_ZSt19__throw_logic_errorPKc@PLT
	.cfi_endproc
.LFE4426:
	.size	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_, .-_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_
	.weak	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC1IS3_EEPKcRKS3_
	.set	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC1IS3_EEPKcRKS3_,_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC2IS3_EEPKcRKS3_
	.section	.rodata._ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_.str1.1,"aMS",@progbits,1
.LC10:
	.string	"vector::_M_realloc_insert"
	.section	.text._ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_,"axG",@progbits,_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_
	.type	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_, @function
_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_:
.LFB4550:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA4550
	endbr64
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	movq	%rdx, %rcx
	movabsq	$288230376151711743, %rdx
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$72, %rsp
	.cfi_def_cfa_offset 128
	movq	8(%rdi), %r12
	movq	(%rdi), %r13
	movq	%fs:40, %rax
	movq	%rax, 56(%rsp)
	xorl	%eax, %eax
	movq	%r12, %rax
	subq	%r13, %rax
	sarq	$5, %rax
	cmpq	%rdx, %rax
	je	.L463
	testq	%rax, %rax
	movl	$1, %edx
	cmovne	%rax, %rdx
	movq	%rsi, %r15
	addq	%rdx, %rax
	setc	%dl
	movzbl	%dl, %edx
	movq	%rax, 24(%rsp)
	movq	%rdi, %r14
	movq	%rsi, %rbp
	movq	%rsi, %rbx
	subq	%r13, %r15
	testq	%rdx, %rdx
	jne	.L444
	testq	%rax, %rax
	jne	.L420
	movq	$0, 16(%rsp)
.L442:
	addq	16(%rsp), %r15
	leaq	16(%r15), %rdi
	movq	(%rcx), %rax
	movq	%rdi, (%r15)
	movq	%r15, (%rsp)
	movq	8(%rcx), %r15
	movq	%rdi, 32(%rsp)
	movq	%rax, %rdi
	addq	%r15, %rdi
	movq	%rax, 40(%rsp)
	je	.L421
	testq	%rax, %rax
	je	.L464
.L421:
	movq	%r15, 48(%rsp)
	cmpq	$15, %r15
	ja	.L465
	cmpq	$1, %r15
	jne	.L424
	movq	40(%rsp), %rax
	movq	(%rsp), %rdi
	movzbl	(%rax), %eax
	movb	%al, 16(%rdi)
.L425:
	movq	(%rsp), %rax
	movq	%r15, 8(%rax)
	movq	32(%rsp), %rax
	movb	$0, (%rax,%r15)
	cmpq	%r13, %rbp
	je	.L445
	movq	%rbp, %rdi
	movq	16(%rsp), %rdx
	subq	%r13, %rdi
	movq	%r13, %rax
	.p2align 4,,10
	.p2align 3
.L430:
	leaq	16(%rdx), %rcx
	movq	%rcx, (%rdx)
	leaq	16(%rax), %rsi
	movq	(%rax), %rcx
	cmpq	%rsi, %rcx
	je	.L466
	movq	%rcx, (%rdx)
	movq	16(%rax), %rcx
	addq	$32, %rax
	movq	%rcx, 16(%rdx)
	movq	-24(%rax), %rcx
	addq	$32, %rdx
	movq	%rcx, -24(%rdx)
	cmpq	%rbp, %rax
	jne	.L430
.L429:
	movq	16(%rsp), %rax
	leaq	(%rax,%rdi), %rsi
.L426:
	addq	$32, %rsi
	cmpq	%r12, %rbp
	je	.L431
	movq	%r12, %rdi
	subq	%rbp, %rdi
	movq	%rsi, %rax
	.p2align 4,,10
	.p2align 3
.L435:
	leaq	16(%rax), %rdx
	movq	%rdx, (%rax)
	movq	(%rbx), %rdx
	leaq	16(%rbx), %rcx
	cmpq	%rcx, %rdx
	je	.L467
	movq	%rdx, (%rax)
	movq	16(%rbx), %rdx
	addq	$32, %rbx
	movq	%rdx, 16(%rax)
	movq	-24(%rbx), %rdx
	addq	$32, %rax
	movq	%rdx, -24(%rax)
	cmpq	%r12, %rbx
	jne	.L435
.L434:
	addq	%rdi, %rsi
.L431:
	vmovq	16(%rsp), %xmm1
	vpinsrq	$1, %rsi, %xmm1, %xmm0
	testq	%r13, %r13
	je	.L436
	movq	16(%r14), %rsi
	movq	%r13, %rdi
	subq	%r13, %rsi
	vmovdqa	%xmm0, (%rsp)
	call	_ZdlPvm@PLT
	vmovdqa	(%rsp), %xmm0
.L436:
	movq	16(%rsp), %rax
	vmovdqu	%xmm0, (%r14)
	addq	24(%rsp), %rax
	movq	%rax, 16(%r14)
	movq	56(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L468
	addq	$72, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L424:
	.cfi_restore_state
	testq	%r15, %r15
	je	.L425
	movq	32(%rsp), %rdi
	jmp	.L423
	.p2align 4,,10
	.p2align 3
.L467:
	vmovdqu	16(%rbx), %xmm3
	movq	8(%rbx), %rdx
	addq	$32, %rbx
	movq	%rdx, 8(%rax)
	vmovdqu	%xmm3, 16(%rax)
	addq	$32, %rax
	cmpq	%rbx, %r12
	jne	.L435
	jmp	.L434
	.p2align 4,,10
	.p2align 3
.L466:
	vmovdqu	16(%rax), %xmm2
	movq	8(%rax), %rcx
	addq	$32, %rax
	movq	%rcx, 8(%rdx)
	vmovdqu	%xmm2, 16(%rdx)
	addq	$32, %rdx
	cmpq	%rax, %rbp
	jne	.L430
	jmp	.L429
	.p2align 4,,10
	.p2align 3
.L444:
	movabsq	$9223372036854775776, %rax
	movq	%rax, 24(%rsp)
	movq	%rax, %rdi
.L419:
	movq	%rcx, (%rsp)
.LEHB0:
	call	_Znwm@PLT
.LEHE0:
	movq	%rax, 16(%rsp)
	movq	(%rsp), %rcx
	jmp	.L442
	.p2align 4,,10
	.p2align 3
.L465:
	movq	(%rsp), %rdi
	leaq	48(%rsp), %rsi
	xorl	%edx, %edx
.LEHB1:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE1:
	movq	(%rsp), %rsi
	movq	%rax, %rdi
	movq	%rax, (%rsi)
	movq	48(%rsp), %rax
	movq	%rax, 16(%rsi)
.L423:
	movq	40(%rsp), %rsi
	movq	%r15, %rdx
	call	memcpy@PLT
	movq	(%rsp), %rax
	movq	48(%rsp), %r15
	movq	(%rax), %rax
	movq	%rax, 32(%rsp)
	jmp	.L425
	.p2align 4,,10
	.p2align 3
.L445:
	movq	16(%rsp), %rsi
	jmp	.L426
.L420:
	movq	%rax, %rdi
	movabsq	$288230376151711743, %rax
	cmpq	%rax, %rdi
	cmovbe	%rdi, %rax
	salq	$5, %rax
	movq	%rax, 24(%rsp)
	movq	%rax, %rdi
	jmp	.L419
.L468:
	call	__stack_chk_fail@PLT
.L463:
	leaq	.LC10(%rip), %rdi
.LEHB2:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE2:
.L464:
	leaq	.LC9(%rip), %rdi
.LEHB3:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE3:
.L446:
	endbr64
	movq	%rax, %rdi
.L437:
	vzeroupper
	call	__cxa_begin_catch@PLT
	cmpq	$0, 16(%rsp)
	je	.L469
	movq	24(%rsp), %rsi
	movq	16(%rsp), %rdi
	call	_ZdlPvm@PLT
.L441:
.LEHB4:
	call	__cxa_rethrow@PLT
.LEHE4:
.L469:
	movq	(%rsp), %rax
	movq	(%rax), %rdi
	cmpq	%rdi, 32(%rsp)
	je	.L441
	movq	16(%rax), %rsi
	incq	%rsi
	call	_ZdlPvm@PLT
	jmp	.L441
.L447:
	endbr64
	movq	%rax, %rbp
.L440:
	vzeroupper
	call	__cxa_end_catch@PLT
	movq	%rbp, %rdi
.LEHB5:
	call	_Unwind_Resume@PLT
.LEHE5:
	.cfi_endproc
.LFE4550:
	.section	.gcc_except_table._ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_,"aG",@progbits,_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_,comdat
	.align 4
.LLSDA4550:
	.byte	0xff
	.byte	0x9b
	.uleb128 .LLSDATT4550-.LLSDATTD4550
.LLSDATTD4550:
	.byte	0x1
	.uleb128 .LLSDACSE4550-.LLSDACSB4550
.LLSDACSB4550:
	.uleb128 .LEHB0-.LFB4550
	.uleb128 .LEHE0-.LEHB0
	.uleb128 0
	.uleb128 0
	.uleb128 .LEHB1-.LFB4550
	.uleb128 .LEHE1-.LEHB1
	.uleb128 .L446-.LFB4550
	.uleb128 0x1
	.uleb128 .LEHB2-.LFB4550
	.uleb128 .LEHE2-.LEHB2
	.uleb128 0
	.uleb128 0
	.uleb128 .LEHB3-.LFB4550
	.uleb128 .LEHE3-.LEHB3
	.uleb128 .L446-.LFB4550
	.uleb128 0x1
	.uleb128 .LEHB4-.LFB4550
	.uleb128 .LEHE4-.LEHB4
	.uleb128 .L447-.LFB4550
	.uleb128 0
	.uleb128 .LEHB5-.LFB4550
	.uleb128 .LEHE5-.LEHB5
	.uleb128 0
	.uleb128 0
.LLSDACSE4550:
	.byte	0x1
	.byte	0
	.align 4
	.long	0

.LLSDATT4550:
	.section	.text._ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_,"axG",@progbits,_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_,comdat
	.size	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_, .-_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_
	.section	.rodata.str1.1,"aMS",@progbits,1
.LC11:
	.string	"Failed to open dictionary: "
	.section	.text.unlikely,"ax",@progbits
.LCOLDB12:
	.text
.LHOTB12:
	.p2align 4
	.globl	_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.type	_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE, @function
_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE:
.LFB3885:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA3885
	endbr64
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	pushq	%r15
	pushq	%r14
	pushq	%r13
	.cfi_offset 15, -24
	.cfi_offset 14, -32
	.cfi_offset 13, -40
	movq	%rdi, %r13
	pushq	%r12
	pushq	%rbx
	.cfi_offset 12, -48
	.cfi_offset 3, -56
	movq	%rsi, %rbx
	andq	$-32, %rsp
	subq	$640, %rsp
	movq	%fs:40, %rax
	movq	%rax, 632(%rsp)
	xorl	%eax, %eax
	movq	$0, (%rdi)
	movq	$0, 8(%rdi)
	movq	$0, 16(%rdi)
	leaq	352(%rsp), %r15
	movq	%r15, %rdi
	movq	%r15, 40(%rsp)
	call	_ZNSt8ios_baseC2Ev@PLT
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, 352(%rsp)
	xorl	%eax, %eax
	movw	%ax, 576(%rsp)
	vpxor	%xmm0, %xmm0, %xmm0
	movq	8+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rax
	vmovdqu	%ymm0, 584(%rsp)
	movq	%rax, 96(%rsp)
	movq	16+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rcx
	movq	-24(%rax), %rax
	movq	$0, 568(%rsp)
	movq	%rcx, 96(%rsp,%rax)
	movq	$0, 104(%rsp)
	movq	8+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rax
	leaq	96(%rsp), %r12
	movq	-24(%rax), %rdi
	xorl	%esi, %esi
	addq	%r12, %rdi
	vzeroupper
.LEHB6:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E@PLT
.LEHE6:
	leaq	24+_ZTVSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rax
	leaq	112(%rsp), %r14
	movq	%rax, 96(%rsp)
	movq	%r14, %rdi
	addq	$40, %rax
	movq	%rax, 352(%rsp)
.LEHB7:
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEEC1Ev@PLT
.LEHE7:
	movq	%r14, %rsi
	movq	%r15, %rdi
.LEHB8:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E@PLT
	movq	(%rbx), %rsi
	movl	$8, %edx
	movq	%r14, %rdi
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE4openEPKcSt13_Ios_Openmode@PLT
	movq	96(%rsp), %rdx
	movq	-24(%rdx), %rdi
	addq	%r12, %rdi
	testq	%rax, %rax
	je	.L526
	xorl	%esi, %esi
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate@PLT
.LEHE8:
.L472:
	leaq	216(%rsp), %rax
	movq	%rax, %rdi
	movq	%rax, 32(%rsp)
	call	_ZNKSt12__basic_fileIcE7is_openEv@PLT
	testb	%al, %al
	je	.L527
	leaq	80(%rsp), %rax
	movq	%rax, 16(%rsp)
	movq	%rax, 64(%rsp)
	movq	$0, 72(%rsp)
	movb	$0, 80(%rsp)
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rbx
	jmp	.L484
	.p2align 4,,10
	.p2align 3
.L531:
	movsbl	67(%r15), %edx
.L494:
	leaq	64(%rsp), %r15
	movq	%r15, %rsi
	movq	%r12, %rdi
.LEHB9:
	call	_ZSt7getlineIcSt11char_traitsIcESaIcEERSt13basic_istreamIT_T0_ES7_RNSt7__cxx1112basic_stringIS4_S5_T1_EES4_@PLT
	movq	(%rax), %rdx
	movq	-24(%rdx), %rdx
	testb	$5, 32(%rax,%rdx)
	jne	.L528
	cmpq	$0, 72(%rsp)
	jne	.L529
.L484:
	movq	96(%rsp), %rax
	movq	-24(%rax), %rax
	movq	336(%rsp,%rax), %r15
	testq	%r15, %r15
	je	.L530
	cmpb	$0, 56(%r15)
	jne	.L531
	movq	%r15, %rdi
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r15), %rax
	movl	$10, %edx
	movq	48(%rax), %rax
	cmpq	%rbx, %rax
	je	.L494
	movl	$10, %esi
	movq	%r15, %rdi
	call	*%rax
	movsbl	%al, %edx
	jmp	.L494
	.p2align 4,,10
	.p2align 3
.L528:
	movq	%r14, %rdi
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE5closeEv@PLT
.LEHE9:
	testq	%rax, %rax
	je	.L532
.L496:
	movq	64(%rsp), %rdi
	cmpq	16(%rsp), %rdi
	je	.L482
	movq	80(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L482:
	leaq	24+_ZTVSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, 96(%rsp)
	addq	$40, %rax
	movq	%rax, 352(%rsp)
	movq	%r14, %rdi
	leaq	16+_ZTVSt13basic_filebufIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, 112(%rsp)
.LEHB10:
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE5closeEv@PLT
.LEHE10:
.L499:
	movq	32(%rsp), %rdi
	call	_ZNSt12__basic_fileIcED1Ev@PLT
	leaq	16+_ZTVSt15basic_streambufIcSt11char_traitsIcEE(%rip), %rax
	leaq	168(%rsp), %rdi
	movq	%rax, 112(%rsp)
	call	_ZNSt6localeD1Ev@PLT
	movq	8+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	16+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rcx
	movq	%rax, 96(%rsp)
	movq	-24(%rax), %rax
	movq	40(%rsp), %rdi
	movq	%rcx, 96(%rsp,%rax)
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, 352(%rsp)
	movq	$0, 104(%rsp)
	call	_ZNSt8ios_baseD2Ev@PLT
	movq	632(%rsp), %rax
	subq	%fs:40, %rax
	jne	.L533
	leaq	-40(%rbp), %rsp
	popq	%rbx
	popq	%r12
	movq	%r13, %rax
	popq	%r13
	popq	%r14
	popq	%r15
	popq	%rbp
	.cfi_remember_state
	.cfi_def_cfa 7, 8
	ret
	.p2align 4,,10
	.p2align 3
.L529:
	.cfi_restore_state
	movq	8(%r13), %rax
	movq	%rax, 24(%rsp)
	cmpq	16(%r13), %rax
	je	.L485
	leaq	16(%rax), %rdi
	movq	%rdi, (%rax)
	movq	72(%rsp), %r15
	movq	64(%rsp), %rax
	movq	%rax, %rcx
	addq	%r15, %rcx
	movq	%rax, 8(%rsp)
	je	.L486
	testq	%rax, %rax
	je	.L534
.L486:
	movq	%r15, 56(%rsp)
	cmpq	$15, %r15
	ja	.L535
	cmpq	$1, %r15
	jne	.L489
	movq	8(%rsp), %rax
	movq	24(%rsp), %rcx
	movzbl	(%rax), %eax
	movb	%al, 16(%rcx)
.L490:
	movq	24(%rsp), %rax
	movq	%r15, 8(%rax)
	movb	$0, (%rdi,%r15)
	addq	$32, 8(%r13)
	jmp	.L484
	.p2align 4,,10
	.p2align 3
.L527:
	leaq	_ZSt4cerr(%rip), %r15
	movl	$27, %edx
	leaq	.LC11(%rip), %rsi
	movq	%r15, %rdi
.LEHB11:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	8(%rbx), %rdx
	movq	(%rbx), %rsi
	movq	%r15, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %rbx
	testq	%rbx, %rbx
	je	.L536
	cmpb	$0, 56(%rbx)
	je	.L480
	movsbl	67(%rbx), %esi
.L481:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE11:
	jmp	.L482
	.p2align 4,,10
	.p2align 3
.L526:
	movl	32(%rdi), %esi
	orl	$4, %esi
.LEHB12:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate@PLT
.LEHE12:
	jmp	.L472
	.p2align 4,,10
	.p2align 3
.L485:
	movq	%rax, %rsi
	movq	%r15, %rdx
	movq	%r13, %rdi
.LEHB13:
	call	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EE17_M_realloc_insertIJRKS5_EEEvN9__gnu_cxx17__normal_iteratorIPS5_S7_EEDpOT_
.LEHE13:
	jmp	.L484
	.p2align 4,,10
	.p2align 3
.L480:
	movq	%rbx, %rdi
.LEHB14:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%rbx), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L481
	movq	%rbx, %rdi
	call	*%rax
.LEHE14:
	movsbl	%al, %esi
	jmp	.L481
	.p2align 4,,10
	.p2align 3
.L489:
	testq	%r15, %r15
	je	.L490
	jmp	.L488
	.p2align 4,,10
	.p2align 3
.L535:
	movq	24(%rsp), %rdi
	leaq	56(%rsp), %rsi
	xorl	%edx, %edx
.LEHB15:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
	movq	24(%rsp), %rcx
	movq	%rax, %rdi
	movq	%rax, (%rcx)
	movq	56(%rsp), %rax
	movq	%rax, 16(%rcx)
.L488:
	movq	8(%rsp), %rsi
	movq	%r15, %rdx
	call	memcpy@PLT
	movq	24(%rsp), %rax
	movq	56(%rsp), %r15
	movq	(%rax), %rdi
	jmp	.L490
.L532:
	movq	96(%rsp), %rax
	movq	-24(%rax), %rdi
	addq	%r12, %rdi
	movl	32(%rdi), %esi
	orl	$4, %esi
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate@PLT
	jmp	.L496
.L530:
	call	_ZSt16__throw_bad_castv@PLT
.L534:
	leaq	.LC9(%rip), %rdi
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE15:
.L533:
	call	__stack_chk_fail@PLT
.L536:
.LEHB16:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE16:
.L510:
	endbr64
	movq	%rax, %r12
	jmp	.L475
.L509:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L476
.L508:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L477
.L511:
	endbr64
	movq	%rax, %rdi
	jmp	.L498
.L506:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L502
.L507:
	endbr64
	movq	%rax, %rbx
	jmp	.L500
	.section	.gcc_except_table,"a",@progbits
	.align 4
.LLSDA3885:
	.byte	0xff
	.byte	0x9b
	.uleb128 .LLSDATT3885-.LLSDATTD3885
.LLSDATTD3885:
	.byte	0x1
	.uleb128 .LLSDACSE3885-.LLSDACSB3885
.LLSDACSB3885:
	.uleb128 .LEHB6-.LFB3885
	.uleb128 .LEHE6-.LEHB6
	.uleb128 .L508-.LFB3885
	.uleb128 0
	.uleb128 .LEHB7-.LFB3885
	.uleb128 .LEHE7-.LEHB7
	.uleb128 .L509-.LFB3885
	.uleb128 0
	.uleb128 .LEHB8-.LFB3885
	.uleb128 .LEHE8-.LEHB8
	.uleb128 .L510-.LFB3885
	.uleb128 0
	.uleb128 .LEHB9-.LFB3885
	.uleb128 .LEHE9-.LEHB9
	.uleb128 .L507-.LFB3885
	.uleb128 0
	.uleb128 .LEHB10-.LFB3885
	.uleb128 .LEHE10-.LEHB10
	.uleb128 .L511-.LFB3885
	.uleb128 0x1
	.uleb128 .LEHB11-.LFB3885
	.uleb128 .LEHE11-.LEHB11
	.uleb128 .L506-.LFB3885
	.uleb128 0
	.uleb128 .LEHB12-.LFB3885
	.uleb128 .LEHE12-.LEHB12
	.uleb128 .L510-.LFB3885
	.uleb128 0
	.uleb128 .LEHB13-.LFB3885
	.uleb128 .LEHE13-.LEHB13
	.uleb128 .L507-.LFB3885
	.uleb128 0
	.uleb128 .LEHB14-.LFB3885
	.uleb128 .LEHE14-.LEHB14
	.uleb128 .L506-.LFB3885
	.uleb128 0
	.uleb128 .LEHB15-.LFB3885
	.uleb128 .LEHE15-.LEHB15
	.uleb128 .L507-.LFB3885
	.uleb128 0
	.uleb128 .LEHB16-.LFB3885
	.uleb128 .LEHE16-.LEHB16
	.uleb128 .L506-.LFB3885
	.uleb128 0
.LLSDACSE3885:
	.byte	0x1
	.byte	0
	.align 4
	.long	0

.LLSDATT3885:
	.text
	.cfi_endproc
	.section	.text.unlikely
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDAC3885
	.type	_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE.cold, @function
_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE.cold:
.LFSB3885:
.L475:
	.cfi_def_cfa 6, 16
	.cfi_offset 3, -56
	.cfi_offset 6, -16
	.cfi_offset 12, -48
	.cfi_offset 13, -40
	.cfi_offset 14, -32
	.cfi_offset 15, -24
	movq	%r14, %rdi
	vzeroupper
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEED1Ev@PLT
.L476:
	movq	8+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	16+_ZTTSt14basic_ifstreamIcSt11char_traitsIcEE(%rip), %rcx
	movq	%rax, 96(%rsp)
	movq	-24(%rax), %rax
	movq	%rcx, 96(%rsp,%rax)
	movq	$0, 104(%rsp)
.L477:
	movq	40(%rsp), %rdi
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, 352(%rsp)
	call	_ZNSt8ios_baseD2Ev@PLT
.L478:
	movq	%r13, %rdi
	call	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED1Ev
	movq	%r12, %rdi
.LEHB17:
	call	_Unwind_Resume@PLT
.LEHE17:
.L498:
	vzeroupper
	call	__cxa_begin_catch@PLT
	call	__cxa_end_catch@PLT
	jmp	.L499
.L500:
	movq	64(%rsp), %rdi
	cmpq	16(%rsp), %rdi
	je	.L524
	movq	80(%rsp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L502:
	movq	%r12, %rdi
	call	_ZNSt14basic_ifstreamIcSt11char_traitsIcEED1Ev@PLT
	movq	%rbx, %r12
	jmp	.L478
.L524:
	vzeroupper
	jmp	.L502
	.cfi_endproc
.LFE3885:
	.section	.gcc_except_table
	.align 4
.LLSDAC3885:
	.byte	0xff
	.byte	0x9b
	.uleb128 .LLSDATTC3885-.LLSDATTDC3885
.LLSDATTDC3885:
	.byte	0x1
	.uleb128 .LLSDACSEC3885-.LLSDACSBC3885
.LLSDACSBC3885:
	.uleb128 .LEHB17-.LCOLDB12
	.uleb128 .LEHE17-.LEHB17
	.uleb128 0
	.uleb128 0
.LLSDACSEC3885:
	.byte	0x1
	.byte	0
	.align 4
	.long	0

.LLSDATTC3885:
	.section	.text.unlikely
	.text
	.size	_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE, .-_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.section	.text.unlikely
	.size	_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE.cold, .-_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE.cold
.LCOLDE12:
	.text
.LHOTE12:
	.section	.text._ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_,"axG",@progbits,_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_
	.type	_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_, @function
_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_:
.LFB4606:
	.cfi_startproc
	endbr64
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	movq	%rdx, %r15
	movabsq	$2305843009213693951, %rdx
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$24, %rsp
	.cfi_def_cfa_offset 80
	movq	8(%rdi), %r12
	movq	(%rdi), %r14
	movq	%r12, %rax
	subq	%r14, %rax
	sarq	$2, %rax
	cmpq	%rdx, %rax
	je	.L558
	testq	%rax, %rax
	movl	$1, %edx
	cmovne	%rax, %rdx
	xorl	%ecx, %ecx
	addq	%rdx, %rax
	setc	%cl
	movq	%rsi, %rdx
	movq	%rdi, %rbp
	movq	%rsi, %r13
	subq	%r14, %rdx
	testq	%rcx, %rcx
	jne	.L550
	testq	%rax, %rax
	jne	.L542
	xorl	%ebx, %ebx
	xorl	%edi, %edi
.L548:
	movl	(%r15), %eax
	subq	%r13, %r12
	leaq	4(%rdi,%rdx), %r15
	movl	%eax, (%rdi,%rdx)
	vmovq	%rdi, %xmm1
	leaq	(%r15,%r12), %rax
	vpinsrq	$1, %rax, %xmm1, %xmm0
	vmovdqa	%xmm0, (%rsp)
	testq	%rdx, %rdx
	jg	.L559
	testq	%r12, %r12
	jg	.L546
	testq	%r14, %r14
	jne	.L557
.L547:
	movq	%rbx, 16(%rbp)
	vmovdqa	(%rsp), %xmm2
	vmovdqu	%xmm2, 0(%rbp)
	addq	$24, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L559:
	.cfi_restore_state
	movq	%r14, %rsi
	call	memmove@PLT
	testq	%r12, %r12
	jg	.L546
.L557:
	movq	16(%rbp), %rsi
	movq	%r14, %rdi
	subq	%r14, %rsi
	call	_ZdlPvm@PLT
	jmp	.L547
	.p2align 4,,10
	.p2align 3
.L546:
	movq	%r12, %rdx
	movq	%r13, %rsi
	movq	%r15, %rdi
	call	memcpy@PLT
	testq	%r14, %r14
	je	.L547
	jmp	.L557
	.p2align 4,,10
	.p2align 3
.L550:
	movabsq	$9223372036854775804, %rbx
.L541:
	movq	%rbx, %rdi
	movq	%rdx, (%rsp)
	call	_Znwm@PLT
	movq	%rax, %rdi
	movq	(%rsp), %rdx
	addq	%rax, %rbx
	jmp	.L548
	.p2align 4,,10
	.p2align 3
.L542:
	movabsq	$2305843009213693951, %rcx
	cmpq	%rcx, %rax
	cmova	%rcx, %rax
	leaq	0(,%rax,4), %rbx
	jmp	.L541
.L558:
	leaq	.LC10(%rip), %rdi
	call	_ZSt20__throw_length_errorPKc@PLT
	.cfi_endproc
.LFE4606:
	.size	_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_, .-_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_
	.text
	.p2align 4
	.type	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.2, @function
_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.2:
.LFB5185:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA5185
	endbr64
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	movq	%rdi, %r12
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$24, %rsp
	.cfi_def_cfa_offset 80
	call	omp_get_num_threads@PLT
	movl	%eax, %ebx
	call	omp_get_thread_num@PLT
	movslq	%eax, %rsi
	movq	32(%r12), %rax
	movslq	%ebx, %rcx
	cqto
	idivq	%rcx
	cmpq	%rdx, %rsi
	jl	.L561
.L568:
	movq	%rax, %rcx
	imulq	%rsi, %rcx
	leaq	(%rcx,%rdx), %rbx
	leaq	(%rax,%rbx), %rbp
	cmpq	%rbp, %rbx
	jge	.L574
	movq	8(%r12), %rax
	leaq	(%rsi,%rsi,2), %r15
	leaq	(%rax,%rbx,4), %rdx
	movq	24(%r12), %r14
	movq	16(%r12), %r13
	salq	$3, %r15
	movabsq	$-7046029254386353131, %rax
	jmp	.L565
	.p2align 4,,10
	.p2align 3
.L564:
	incq	%rbx
	addq	$4, %rdx
	cmpq	%rbx, %rbp
	je	.L574
.L565:
	movl	(%r14,%rbx,4), %edi
	cmpl	%edi, 0(%r13,%rbx,4)
	jge	.L564
	movslq	(%rdx), %rcx
	movq	(%r12), %rsi
	movq	%rcx, %rdi
	imulq	%rax, %rcx
	movq	24(%rsi), %r8
	movq	(%rsi), %r9
	andq	%r8, %rcx
	leaq	(%r9,%rcx,8), %rsi
	cmpb	$0, 4(%rsi)
	jne	.L567
	jmp	.L564
	.p2align 4,,10
	.p2align 3
.L576:
	incq	%rcx
	andq	%r8, %rcx
	leaq	(%r9,%rcx,8), %rsi
	cmpb	$0, 4(%rsi)
	je	.L564
.L567:
	cmpl	(%rsi), %edi
	jne	.L576
	movq	40(%r12), %rcx
	movq	(%rcx), %r8
	addq	%r15, %r8
	movq	8(%r8), %rsi
	cmpq	16(%r8), %rsi
	je	.L577
	movl	%edi, (%rsi)
	incq	%rbx
	addq	$4, %rsi
	movq	%rsi, 8(%r8)
	addq	$4, %rdx
	cmpq	%rbx, %rbp
	jne	.L565
.L574:
	addq	$24, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L561:
	.cfi_restore_state
	incq	%rax
	xorl	%edx, %edx
	jmp	.L568
.L577:
	movq	%r8, %rdi
	movq	%rdx, 8(%rsp)
	call	_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_
	movq	8(%rsp), %rdx
	movabsq	$-7046029254386353131, %rax
	jmp	.L564
	.cfi_endproc
.LFE5185:
	.section	.gcc_except_table
.LLSDA5185:
	.byte	0xff
	.byte	0xff
	.byte	0x1
	.uleb128 .LLSDACSE5185-.LLSDACSB5185
.LLSDACSB5185:
.LLSDACSE5185:
	.text
	.size	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.2, .-_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.2
	.p2align 4
	.type	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.1, @function
_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.1:
.LFB5184:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA5184
	endbr64
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	movq	%rdi, %r13
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$8, %rsp
	.cfi_def_cfa_offset 64
	call	omp_get_num_threads@PLT
	movl	%eax, %ebx
	call	omp_get_thread_num@PLT
	movslq	%eax, %rsi
	movq	16(%r13), %rax
	movslq	%ebx, %rcx
	cqto
	idivq	%rcx
	cmpq	%rdx, %rsi
	jl	.L579
.L585:
	movq	%rax, %rcx
	imulq	%rsi, %rcx
	leaq	(%rcx,%rdx), %rbx
	leaq	(%rax,%rbx), %rbp
	cmpq	%rbp, %rbx
	jge	.L587
	movq	0(%r13), %rax
	leaq	(%rsi,%rsi,2), %r15
	movq	8(%r13), %r14
	salq	$3, %r15
	leaq	(%rax,%rbx,4), %r12
	jmp	.L582
	.p2align 4,,10
	.p2align 3
.L584:
	incq	%rbx
	addq	$4, %r12
	cmpq	%rbx, %rbp
	je	.L587
.L582:
	movl	(%r14,%rbx,4), %eax
	leal	-8582(%rax), %edx
	cmpl	$91, %edx
	ja	.L584
	movq	24(%r13), %rax
	movq	(%rax), %rdi
	addq	%r15, %rdi
	movq	8(%rdi), %rsi
	cmpq	16(%rdi), %rsi
	je	.L583
	movl	(%r12), %eax
	addq	$4, %rsi
	incq	%rbx
	movl	%eax, -4(%rsi)
	addq	$4, %r12
	movq	%rsi, 8(%rdi)
	cmpq	%rbx, %rbp
	jne	.L582
.L587:
	addq	$8, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L583:
	.cfi_restore_state
	movq	%r12, %rdx
	call	_ZNSt6vectorIiSaIiEE17_M_realloc_insertIJRKiEEEvN9__gnu_cxx17__normal_iteratorIPiS1_EEDpOT_
	jmp	.L584
	.p2align 4,,10
	.p2align 3
.L579:
	incq	%rax
	xorl	%edx, %edx
	jmp	.L585
	.cfi_endproc
.LFE5184:
	.section	.gcc_except_table
.LLSDA5184:
	.byte	0xff
	.byte	0xff
	.byte	0x1
	.uleb128 .LLSDACSE5184-.LLSDACSB5184
.LLSDACSB5184:
.LLSDACSE5184:
	.text
	.size	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.1, .-_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.1
	.section	.rodata._ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm.str1.1,"aMS",@progbits,1
.LC13:
	.string	"vector::_M_default_append"
	.section	.text._ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm,"axG",@progbits,_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm
	.type	_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm, @function
_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm:
.LFB4763:
	.cfi_startproc
	endbr64
	testq	%rsi, %rsi
	je	.L615
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	movabsq	$1152921504606846975, %rax
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	movq	%rdi, %r12
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	movq	%rsi, %rbp
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$8, %rsp
	.cfi_def_cfa_offset 64
	movq	8(%rdi), %rcx
	movq	(%rdi), %rdi
	movq	%rcx, %rbx
	subq	%rdi, %rbx
	movq	%rbx, %r13
	movq	16(%r12), %r8
	sarq	$3, %r13
	subq	%r13, %rax
	movq	%rax, %rdx
	movq	%r8, %rax
	subq	%rcx, %rax
	sarq	$3, %rax
	cmpq	%rax, %rsi
	ja	.L591
	movq	%rsi, %rdx
	movq	%rcx, %rax
	.p2align 4,,10
	.p2align 3
.L592:
	movl	$0, (%rax)
	movb	$0, 4(%rax)
	addq	$8, %rax
	decq	%rdx
	jne	.L592
	leaq	(%rcx,%rbp,8), %rax
	movq	%rax, 8(%r12)
	addq	$8, %rsp
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L615:
	.cfi_restore 3
	.cfi_restore 6
	.cfi_restore 12
	.cfi_restore 13
	.cfi_restore 14
	.cfi_restore 15
	ret
	.p2align 4,,10
	.p2align 3
.L591:
	.cfi_def_cfa_offset 64
	.cfi_offset 3, -56
	.cfi_offset 6, -48
	.cfi_offset 12, -40
	.cfi_offset 13, -32
	.cfi_offset 14, -24
	.cfi_offset 15, -16
	cmpq	%rsi, %rdx
	jb	.L618
	cmpq	%r13, %rsi
	movq	%r13, %rax
	cmovnb	%rsi, %rax
	addq	%r13, %rax
	jc	.L595
	testq	%rax, %rax
	jne	.L619
	xorl	%r14d, %r14d
	xorl	%r15d, %r15d
.L597:
	leaq	(%r15,%rbx), %rax
	movq	%rbp, %rdx
	.p2align 4,,10
	.p2align 3
.L598:
	movl	$0, (%rax)
	movb	$0, 4(%rax)
	addq	$8, %rax
	decq	%rdx
	jne	.L598
	cmpq	%rcx, %rdi
	je	.L603
	subq	%rdi, %rcx
	addq	%r15, %rcx
	movq	%r15, %rax
	movq	%rdi, %rdx
	.p2align 4,,10
	.p2align 3
.L602:
	movq	(%rdx), %rsi
	addq	$8, %rax
	movq	%rsi, -8(%rax)
	addq	$8, %rdx
	cmpq	%rcx, %rax
	jne	.L602
.L603:
	testq	%rdi, %rdi
	je	.L601
	movq	%r8, %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L601:
	addq	%rbp, %r13
	movq	%r14, 16(%r12)
	leaq	(%r15,%r13,8), %rax
	vmovq	%r15, %xmm1
	vpinsrq	$1, %rax, %xmm1, %xmm0
	vmovdqu	%xmm0, (%r12)
	addq	$8, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
.L619:
	.cfi_restore_state
	movabsq	$1152921504606846975, %rdx
	cmpq	%rdx, %rax
	cmova	%rdx, %rax
	leaq	0(,%rax,8), %r14
.L596:
	movq	%r14, %rdi
	call	_Znwm@PLT
	movq	%rax, %r15
	movq	8(%r12), %rcx
	movq	(%r12), %rdi
	movq	16(%r12), %r8
	addq	%rax, %r14
	jmp	.L597
.L595:
	movabsq	$9223372036854775800, %r14
	jmp	.L596
.L618:
	leaq	.LC13(%rip), %rdi
	call	_ZSt20__throw_length_errorPKc@PLT
	.cfi_endproc
.LFE4763:
	.size	_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm, .-_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm
	.section	.text._ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_,"axG",@progbits,_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_,comdat
	.align 2
	.p2align 4
	.weak	_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_
	.type	_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_, @function
_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_:
.LFB4809:
	.cfi_startproc
	endbr64
	pushq	%r15
	.cfi_def_cfa_offset 16
	.cfi_offset 15, -16
	movq	%rdx, %r15
	movabsq	$-3689348814741910323, %rdx
	pushq	%r14
	.cfi_def_cfa_offset 24
	.cfi_offset 14, -24
	pushq	%r13
	.cfi_def_cfa_offset 32
	.cfi_offset 13, -32
	pushq	%r12
	.cfi_def_cfa_offset 40
	.cfi_offset 12, -40
	pushq	%rbp
	.cfi_def_cfa_offset 48
	.cfi_offset 6, -48
	pushq	%rbx
	.cfi_def_cfa_offset 56
	.cfi_offset 3, -56
	subq	$24, %rsp
	.cfi_def_cfa_offset 80
	movq	8(%rdi), %rbp
	movq	(%rdi), %r13
	movq	%rbp, %rax
	subq	%r13, %rax
	sarq	$3, %rax
	imulq	%rdx, %rax
	movabsq	$230584300921369395, %rdx
	cmpq	%rdx, %rax
	je	.L647
	testq	%rax, %rax
	movl	$1, %edx
	cmovne	%rax, %rdx
	xorl	%ecx, %ecx
	addq	%rdx, %rax
	setc	%cl
	movq	%rsi, %rdx
	movq	%rdi, %r12
	movq	%rsi, %rbx
	subq	%r13, %rdx
	testq	%rcx, %rcx
	jne	.L640
	testq	%rax, %rax
	jne	.L625
	movl	$40, %r8d
	xorl	%r14d, %r14d
	xorl	%edi, %edi
.L639:
	leaq	(%rdi,%rdx), %rax
	leaq	16(%rax), %rdx
	movq	(%r15), %rcx
	movq	%rdx, (%rax)
	leaq	16(%r15), %rdx
	cmpq	%rdx, %rcx
	je	.L648
	movq	%rcx, (%rax)
	movq	16(%r15), %rcx
	movq	%rcx, 16(%rax)
.L627:
	movq	8(%r15), %rcx
	movq	%rdx, (%r15)
	movq	32(%r15), %rdx
	movq	%rcx, 8(%rax)
	movq	$0, 8(%r15)
	movb	$0, 16(%r15)
	movq	%rdx, 32(%rax)
	cmpq	%r13, %rbx
	je	.L628
	movq	%r13, %rax
	movq	%rdi, %rdx
	.p2align 4,,10
	.p2align 3
.L632:
	leaq	16(%rdx), %rcx
	movq	%rcx, (%rdx)
	leaq	16(%rax), %rsi
	movq	(%rax), %rcx
	cmpq	%rsi, %rcx
	je	.L649
	movq	%rcx, (%rdx)
	movq	16(%rax), %rcx
	addq	$40, %rax
	movq	%rcx, 16(%rdx)
	movq	-32(%rax), %rcx
	addq	$40, %rdx
	movq	%rcx, -32(%rdx)
	movq	-8(%rax), %rcx
	movq	%rcx, -8(%rdx)
	cmpq	%rbx, %rax
	jne	.L632
.L631:
	leaq	-40(%rbx), %rax
	subq	%r13, %rax
	movabsq	$922337203685477581, %rdx
	shrq	$3, %rax
	imulq	%rdx, %rax
	movabsq	$2305843009213693951, %rdx
	andq	%rdx, %rax
	leaq	10(%rax,%rax,4), %rax
	leaq	(%rdi,%rax,8), %r8
.L628:
	cmpq	%rbp, %rbx
	je	.L633
	movq	%r8, %rdx
	movq	%rbx, %rax
	.p2align 4,,10
	.p2align 3
.L637:
	leaq	16(%rdx), %rcx
	movq	%rcx, (%rdx)
	movq	(%rax), %rcx
	leaq	16(%rax), %rsi
	cmpq	%rsi, %rcx
	je	.L650
	movq	%rcx, (%rdx)
	movq	16(%rax), %rcx
	addq	$40, %rax
	movq	%rcx, 16(%rdx)
	movq	-32(%rax), %rcx
	addq	$40, %rdx
	movq	%rcx, -32(%rdx)
	movq	-8(%rax), %rcx
	movq	%rcx, -8(%rdx)
	cmpq	%rbp, %rax
	jne	.L637
.L636:
	subq	%rbx, %rbp
	leaq	-40(%rbp), %rax
	movabsq	$922337203685477581, %rdx
	shrq	$3, %rax
	imulq	%rdx, %rax
	movabsq	$2305843009213693951, %rdx
	andq	%rdx, %rax
	leaq	5(%rax,%rax,4), %rax
	leaq	(%r8,%rax,8), %r8
.L633:
	vmovq	%rdi, %xmm1
	vpinsrq	$1, %r8, %xmm1, %xmm0
	testq	%r13, %r13
	je	.L638
	movq	16(%r12), %rsi
	movq	%r13, %rdi
	subq	%r13, %rsi
	vmovdqa	%xmm0, (%rsp)
	call	_ZdlPvm@PLT
	vmovdqa	(%rsp), %xmm0
.L638:
	movq	%r14, 16(%r12)
	vmovdqu	%xmm0, (%r12)
	addq	$24, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 56
	popq	%rbx
	.cfi_def_cfa_offset 48
	popq	%rbp
	.cfi_def_cfa_offset 40
	popq	%r12
	.cfi_def_cfa_offset 32
	popq	%r13
	.cfi_def_cfa_offset 24
	popq	%r14
	.cfi_def_cfa_offset 16
	popq	%r15
	.cfi_def_cfa_offset 8
	ret
	.p2align 4,,10
	.p2align 3
.L649:
	.cfi_restore_state
	movq	8(%rax), %rcx
	vmovdqu	16(%rax), %xmm2
	movq	%rcx, 8(%rdx)
	vmovdqu	%xmm2, 16(%rdx)
	addq	$40, %rax
	movq	-8(%rax), %rcx
	addq	$40, %rdx
	movq	%rcx, -8(%rdx)
	cmpq	%rax, %rbx
	jne	.L632
	jmp	.L631
	.p2align 4,,10
	.p2align 3
.L650:
	movq	8(%rax), %rcx
	vmovdqu	16(%rax), %xmm3
	movq	%rcx, 8(%rdx)
	movq	32(%rax), %rcx
	addq	$40, %rax
	movq	%rcx, 32(%rdx)
	vmovdqu	%xmm3, 16(%rdx)
	addq	$40, %rdx
	cmpq	%rax, %rbp
	jne	.L637
	jmp	.L636
	.p2align 4,,10
	.p2align 3
.L640:
	movabsq	$9223372036854775800, %r14
.L624:
	movq	%r14, %rdi
	movq	%rdx, (%rsp)
	call	_Znwm@PLT
	movq	%rax, %rdi
	movq	(%rsp), %rdx
	addq	%rax, %r14
	leaq	40(%rax), %r8
	jmp	.L639
	.p2align 4,,10
	.p2align 3
.L648:
	vmovdqu	16(%r15), %xmm4
	vmovdqu	%xmm4, 16(%rax)
	jmp	.L627
.L625:
	movabsq	$230584300921369395, %rcx
	cmpq	%rcx, %rax
	cmova	%rcx, %rax
	leaq	(%rax,%rax,4), %r14
	salq	$3, %r14
	jmp	.L624
.L647:
	leaq	.LC10(%rip), %rdi
	call	_ZSt20__throw_length_errorPKc@PLT
	.cfi_endproc
.LFE4809:
	.size	_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_, .-_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_
	.section	.rodata.str1.1
.LC14:
	.string	"basic_string::append"
.LC15:
	.string	"/orders"
.LC16:
	.string	"/o_orderkey.bin"
.LC17:
	.string	"/o_orderdate.bin"
.LC18:
	.string	"/o_orderpriority.bin"
.LC19:
	.string	"Failed to load orders files"
	.section	.rodata.str1.8,"aMS",@progbits,1
	.align 8
.LC21:
	.string	"[TIMING] orders_load_columns: %.2f ms\n"
	.section	.rodata.str1.1
.LC22:
	.string	"/o_orderpriority_dict.txt"
.LC23:
	.string	"Loaded "
.LC24:
	.string	" priorities"
.LC25:
	.string	"  Priority "
.LC26:
	.string	": "
	.section	.rodata.str1.8
	.align 8
.LC27:
	.string	"cannot create std::vector larger than max_size()"
	.align 8
.LC28:
	.string	"[TIMING] orders_filter: %.2f ms\n"
	.section	.rodata.str1.1
.LC29:
	.string	"Filtered orders: "
.LC30:
	.string	" rows"
.LC31:
	.string	"/lineitem"
.LC32:
	.string	"/l_orderkey.bin"
.LC33:
	.string	"/l_commitdate.bin"
.LC34:
	.string	"/l_receiptdate.bin"
.LC35:
	.string	"Failed to load lineitem files"
	.section	.rodata.str1.8
	.align 8
.LC36:
	.string	"[TIMING] lineitem_load_columns: %.2f ms\n"
	.align 8
.LC43:
	.string	"[TIMING] semi_join_build: %.2f ms\n"
	.section	.rodata.str1.1
.LC44:
	.string	"Semi-join set size: "
.LC45:
	.string	" distinct order keys"
	.section	.rodata.str1.8
	.align 8
.LC46:
	.string	"[TIMING] orders_scan_filter_join: %.2f ms\n"
	.section	.rodata.str1.1
.LC47:
	.string	"/Q4.csv"
.LC48:
	.string	"Failed to open output file: "
.LC49:
	.string	"o_orderpriority,order_count\n"
.LC50:
	.string	","
.LC51:
	.string	"\n"
.LC52:
	.string	"[TIMING] output: %.2f ms\n"
.LC53:
	.string	"[TIMING] total: %.2f ms\n"
.LC54:
	.string	"Q4 results written to "
	.section	.text.unlikely
.LCOLDB57:
	.text
.LHOTB57:
	.p2align 4
	.globl	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_
	.type	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_, @function
_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_:
.LFB3923:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA3923
	endbr64
	leaq	8(%rsp), %r10
	.cfi_def_cfa 10, 0
	andq	$-32, %rsp
	pushq	-8(%r10)
	pushq	%rbp
	movq	%rsp, %rbp
	.cfi_escape 0x10,0x6,0x2,0x76,0
	pushq	%r15
	pushq	%r14
	pushq	%r13
	pushq	%r12
	pushq	%r10
	.cfi_escape 0xf,0x3,0x76,0x58,0x6
	.cfi_escape 0x10,0xf,0x2,0x76,0x78
	.cfi_escape 0x10,0xe,0x2,0x76,0x70
	.cfi_escape 0x10,0xd,0x2,0x76,0x68
	.cfi_escape 0x10,0xc,0x2,0x76,0x60
	pushq	%rbx
	.cfi_escape 0x10,0x3,0x2,0x76,0x50
	movq	%rdi, %rbx
	subq	$1408, %rsp
	movq	%rdi, -1304(%rbp)
	movq	%rsi, -1344(%rbp)
	movq	%fs:40, %rax
	movq	%rax, -56(%rbp)
	xorl	%eax, %eax
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	%rax, -1376(%rbp)
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	(%rbx), %r13
	movq	8(%rbx), %r12
	movq	%rax, %r14
	leaq	-736(%rbp), %rax
	movq	%rax, -1272(%rbp)
	movq	%rax, -752(%rbp)
	movq	%r13, %rax
	addq	%r12, %rax
	je	.L652
	testq	%r13, %r13
	je	.L1381
.L652:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1382
	cmpq	$1, %r12
	jne	.L655
	movzbl	0(%r13), %eax
	movb	%al, -736(%rbp)
	movq	-1272(%rbp), %rax
.L656:
	movq	%r12, -744(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-744(%rbp), %rax
	cmpq	$6, %rax
	jbe	.L1383
	leaq	-752(%rbp), %rdi
	movl	$7, %edx
	leaq	.LC15(%rip), %rsi
.LEHB18:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE18:
	movq	-752(%rbp), %r13
	movq	-744(%rbp), %r12
	movq	%r13, %rax
	leaq	-640(%rbp), %rbx
	addq	%r12, %rax
	movq	%rbx, -656(%rbp)
	je	.L661
	testq	%r13, %r13
	je	.L1384
.L661:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1385
	cmpq	$1, %r12
	jne	.L664
	movzbl	0(%r13), %eax
	movb	%al, -640(%rbp)
	movq	%rbx, %rax
.L665:
	movq	%r12, -648(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-648(%rbp), %rax
	cmpq	$14, %rax
	jbe	.L1386
	leaq	-656(%rbp), %rax
	movl	$15, %edx
	leaq	.LC16(%rip), %rsi
	movq	%rax, %rdi
	movq	%rax, -1248(%rbp)
.LEHB19:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE19:
	movq	-656(%rbp), %rdi
	xorl	%esi, %esi
	xorl	%eax, %eax
	movq	$0, -1216(%rbp)
	movq	$0, -1208(%rbp)
.LEHB20:
	call	open@PLT
.LEHE20:
	movl	%eax, -1200(%rbp)
	movl	%eax, %r13d
	testl	%eax, %eax
	js	.L1387
	leaq	-592(%rbp), %rsi
	movl	%eax, %edi
	call	fstat@PLT
	testl	%eax, %eax
	js	.L1388
	movq	-544(%rbp), %rsi
	xorl	%r9d, %r9d
	movl	%r13d, %r8d
	movl	$1, %ecx
	movl	$1, %edx
	xorl	%edi, %edi
	movq	%rsi, -1208(%rbp)
	call	mmap@PLT
	movq	%rax, -1216(%rbp)
	cmpq	$-1, %rax
	je	.L1389
.L680:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L685
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L685:
	movq	-752(%rbp), %r13
	movq	-744(%rbp), %r12
	movq	%r13, %rax
	addq	%r12, %rax
	movq	%rbx, -656(%rbp)
	je	.L686
	testq	%r13, %r13
	je	.L1390
.L686:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1391
	cmpq	$1, %r12
	jne	.L689
	movzbl	0(%r13), %eax
	movb	%al, -640(%rbp)
	movq	%rbx, %rax
.L690:
	movq	%r12, -648(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-648(%rbp), %rax
	cmpq	$15, %rax
	jbe	.L1392
	movq	-1248(%rbp), %rdi
	movl	$16, %edx
	leaq	.LC17(%rip), %rsi
.LEHB21:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE21:
	movq	-656(%rbp), %rdi
	xorl	%esi, %esi
	xorl	%eax, %eax
	movq	$0, -1184(%rbp)
	movq	$0, -1176(%rbp)
.LEHB22:
	call	open@PLT
.LEHE22:
	movl	%eax, -1168(%rbp)
	movl	%eax, %r13d
	testl	%eax, %eax
	js	.L1393
	leaq	-592(%rbp), %rsi
	movl	%eax, %edi
	call	fstat@PLT
	testl	%eax, %eax
	js	.L1394
	movq	-544(%rbp), %rsi
	xorl	%r9d, %r9d
	movl	%r13d, %r8d
	movl	$1, %ecx
	movl	$1, %edx
	xorl	%edi, %edi
	movq	%rsi, -1176(%rbp)
	call	mmap@PLT
	movq	%rax, -1184(%rbp)
	cmpq	$-1, %rax
	je	.L1395
.L705:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L710
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L710:
	movq	-752(%rbp), %r13
	movq	-744(%rbp), %r12
	movq	%r13, %rax
	addq	%r12, %rax
	movq	%rbx, -656(%rbp)
	je	.L711
	testq	%r13, %r13
	je	.L1396
.L711:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1397
	cmpq	$1, %r12
	jne	.L714
	movzbl	0(%r13), %eax
	movb	%al, -640(%rbp)
	movq	%rbx, %rax
.L715:
	movq	%r12, -648(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-648(%rbp), %rax
	cmpq	$19, %rax
	jbe	.L1398
	movq	-1248(%rbp), %rdi
	movl	$20, %edx
	leaq	.LC18(%rip), %rsi
.LEHB23:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE23:
	movq	-656(%rbp), %rdi
	xorl	%esi, %esi
	xorl	%eax, %eax
	movq	$0, -1152(%rbp)
	movq	$0, -1144(%rbp)
.LEHB24:
	call	open@PLT
.LEHE24:
	movl	%eax, -1136(%rbp)
	movl	%eax, %r13d
	testl	%eax, %eax
	js	.L1399
	leaq	-592(%rbp), %rsi
	movl	%eax, %edi
	call	fstat@PLT
	testl	%eax, %eax
	js	.L1400
	movq	-544(%rbp), %rsi
	xorl	%r9d, %r9d
	movl	%r13d, %r8d
	movl	$1, %ecx
	movl	$1, %edx
	xorl	%edi, %edi
	movq	%rsi, -1144(%rbp)
	call	mmap@PLT
	movq	%rax, -1152(%rbp)
	cmpq	$-1, %rax
	je	.L1401
.L730:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L735
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L735:
	movq	-1216(%rbp), %rax
	movq	%rax, -1296(%rbp)
	testq	%rax, %rax
	je	.L736
	movq	-1184(%rbp), %rax
	movq	%rax, -1288(%rbp)
	testq	%rax, %rax
	je	.L736
	movq	-1152(%rbp), %rax
	movq	%rax, -1264(%rbp)
	testq	%rax, %rax
	je	.L736
	movq	-1208(%rbp), %rax
	movq	%rax, -1408(%rbp)
	shrq	$2, %rax
	movq	%rax, -1336(%rbp)
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	%r14, %rax
	vxorpd	%xmm5, %xmm5, %xmm5
	vcvtsi2sdq	%rax, %xmm5, %xmm0
	leaq	.LC21(%rip), %rsi
	movl	$1, %edi
	movl	$1, %eax
	vdivsd	.LC20(%rip), %xmm0, %xmm0
.LEHB25:
	call	__printf_chk@PLT
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	-752(%rbp), %r13
	movq	%rax, -1312(%rbp)
	movq	-744(%rbp), %r12
	leaq	-576(%rbp), %rax
	movq	%rax, -1240(%rbp)
	movq	%rax, -592(%rbp)
	movq	%r13, %rax
	addq	%r12, %rax
	je	.L742
	testq	%r13, %r13
	je	.L1402
.L742:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1403
	cmpq	$1, %r12
	jne	.L745
	movzbl	0(%r13), %eax
	movb	%al, -576(%rbp)
	movq	-1240(%rbp), %rax
	jmp	.L746
.L655:
	testq	%r12, %r12
	jne	.L1404
	movq	-1272(%rbp), %rax
	jmp	.L656
.L736:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$27, %edx
	leaq	.LC19(%rip), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	_ZSt4cerr(%rip), %rax
	movq	-24(%rax), %rax
	movq	240(%r12,%rax), %r13
	testq	%r13, %r13
	je	.L1405
	cmpb	$0, 56(%r13)
	je	.L739
	movsbl	67(%r13), %esi
.L740:
	movq	%r12, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE25:
	movq	-1184(%rbp), %rax
	movq	%rax, -1288(%rbp)
	movq	-1152(%rbp), %rax
	movq	%rax, -1264(%rbp)
.L741:
	movq	-1264(%rbp), %rax
	decq	%rax
	cmpq	$-3, %rax
	jbe	.L1406
.L1009:
	movl	-1136(%rbp), %edi
	testl	%edi, %edi
	js	.L1010
	call	close@PLT
.L1010:
	movq	-1288(%rbp), %rax
	decq	%rax
	cmpq	$-3, %rax
	jbe	.L1407
.L1011:
	movl	-1168(%rbp), %edi
	testl	%edi, %edi
	js	.L1012
	call	close@PLT
.L1012:
	movq	-1296(%rbp), %rax
	decq	%rax
	cmpq	$-3, %rax
	jbe	.L1408
.L1013:
	movl	-1200(%rbp), %edi
	testl	%edi, %edi
	js	.L1014
	call	close@PLT
.L1014:
	movq	-752(%rbp), %rdi
	cmpq	-1272(%rbp), %rdi
	je	.L651
	movq	-736(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L651:
	movq	-56(%rbp), %rax
	subq	%fs:40, %rax
	jne	.L1409
	addq	$1408, %rsp
	popq	%rbx
	popq	%r10
	.cfi_remember_state
	.cfi_def_cfa 10, 0
	popq	%r12
	popq	%r13
	popq	%r14
	popq	%r15
	popq	%rbp
	leaq	-8(%r10), %rsp
	.cfi_def_cfa 7, 8
	ret
.L689:
	.cfi_restore_state
	testq	%r12, %r12
	jne	.L1410
	movq	%rbx, %rax
	jmp	.L690
.L664:
	testq	%r12, %r12
	jne	.L1411
	movq	%rbx, %rax
	jmp	.L665
.L1385:
	leaq	-800(%rbp), %rsi
	leaq	-656(%rbp), %rdi
	xorl	%edx, %edx
.LEHB26:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE26:
	movq	%rax, -656(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -640(%rbp)
.L663:
	movq	%r12, %rdx
	movq	%r13, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-656(%rbp), %rax
	jmp	.L665
.L1382:
	leaq	-752(%rbp), %rdi
	leaq	-800(%rbp), %rsi
	xorl	%edx, %edx
.LEHB27:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE27:
	movq	%rax, -752(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -736(%rbp)
.L654:
	movq	%r12, %rdx
	movq	%r13, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-752(%rbp), %rax
	jmp	.L656
.L1391:
	movq	-1248(%rbp), %rdi
	leaq	-800(%rbp), %rsi
	xorl	%edx, %edx
.LEHB28:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE28:
	movq	%rax, -656(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -640(%rbp)
.L688:
	movq	%r12, %rdx
	movq	%r13, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-656(%rbp), %rax
	jmp	.L690
.L714:
	testq	%r12, %r12
	jne	.L1412
	movq	%rbx, %rax
	jmp	.L715
.L1397:
	movq	-1248(%rbp), %rdi
	leaq	-800(%rbp), %rsi
	xorl	%edx, %edx
.LEHB29:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE29:
	movq	%rax, -656(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -640(%rbp)
.L713:
	movq	%r12, %rdx
	movq	%r13, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-656(%rbp), %rax
	jmp	.L715
.L1399:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$16, %edx
	leaq	.LC6(%rip), %rsi
	movq	%r12, %rdi
.LEHB30:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r13
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r13,%rax), %r12
	testq	%r12, %r12
	je	.L1413
	cmpb	$0, 56(%r12)
	je	.L723
	movsbl	67(%r12), %esi
.L724:
	movq	%r13, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE30:
	jmp	.L730
.L1393:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$16, %edx
	leaq	.LC6(%rip), %rsi
	movq	%r12, %rdi
.LEHB31:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r13
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r13,%rax), %r12
	testq	%r12, %r12
	je	.L1414
	cmpb	$0, 56(%r12)
	je	.L698
	movsbl	67(%r12), %esi
.L699:
	movq	%r13, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE31:
	jmp	.L705
.L1387:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$16, %edx
	leaq	.LC6(%rip), %rsi
	movq	%r12, %rdi
.LEHB32:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r13
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r13,%rax), %r12
	testq	%r12, %r12
	je	.L1415
	cmpb	$0, 56(%r12)
	je	.L673
	movsbl	67(%r12), %esi
.L674:
	movq	%r13, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE32:
	jmp	.L680
.L1408:
	movq	-1208(%rbp), %rsi
	movq	-1296(%rbp), %rdi
	call	munmap@PLT
	jmp	.L1013
.L1407:
	movq	-1176(%rbp), %rsi
	movq	-1288(%rbp), %rdi
	call	munmap@PLT
	jmp	.L1011
.L1406:
	movq	-1144(%rbp), %rsi
	movq	-1264(%rbp), %rdi
	call	munmap@PLT
	jmp	.L1009
.L739:
	movq	%r13, %rdi
.LEHB33:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	0(%r13), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L740
	movq	%r13, %rdi
	call	*%rax
.LEHE33:
	movsbl	%al, %esi
	jmp	.L740
.L745:
	testq	%r12, %r12
	jne	.L1416
	movq	-1240(%rbp), %rax
.L746:
	movq	%r12, -584(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-584(%rbp), %rax
	cmpq	$24, %rax
	jbe	.L1417
	leaq	-592(%rbp), %r15
	movl	$25, %edx
	leaq	.LC22(%rip), %rsi
	movq	%r15, %rdi
.LEHB34:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE34:
	leaq	-1120(%rbp), %rax
	movq	%r15, %rsi
	movq	%rax, %rdi
	movq	%rax, -1432(%rbp)
.LEHB35:
	call	_Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
.LEHE35:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L752
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L752:
	leaq	_ZSt4cout(%rip), %r14
	movl	$7, %edx
	leaq	.LC23(%rip), %rsi
	movq	%r14, %rdi
.LEHB36:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-1112(%rbp), %rsi
	movq	%r14, %rdi
	subq	-1120(%rbp), %rsi
	sarq	$5, %rsi
	call	_ZNSo9_M_insertImEERSoT_@PLT
	movl	$11, %edx
	leaq	.LC24(%rip), %rsi
	movq	%rax, %rdi
	movq	%rax, %r13
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	0(%r13), %rax
	movq	-24(%rax), %rax
	movq	240(%r13,%rax), %r12
	testq	%r12, %r12
	je	.L1418
	cmpb	$0, 56(%r12)
	je	.L754
	movsbl	67(%r12), %esi
.L755:
	movq	%r13, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movq	-1112(%rbp), %rax
	xorl	%ebx, %ebx
	cmpq	%rax, -1120(%rbp)
	jne	.L756
	jmp	.L762
	.p2align 4,,10
	.p2align 3
.L1420:
	movsbl	67(%r13), %esi
.L761:
	movq	%r12, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movq	-1112(%rbp), %rax
	incq	%rbx
	subq	-1120(%rbp), %rax
	sarq	$5, %rax
	cmpq	%rax, %rbx
	jnb	.L762
.L756:
	movl	$11, %edx
	leaq	.LC25(%rip), %rsi
	movq	%r14, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rbx, %rsi
	movq	%r14, %rdi
	call	_ZNSo9_M_insertImEERSoT_@PLT
	movl	$2, %edx
	leaq	.LC26(%rip), %rsi
	movq	%rax, %rdi
	movq	%rax, %r12
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rbx, %rax
	salq	$5, %rax
	addq	-1120(%rbp), %rax
	movq	8(%rax), %rdx
	movq	(%rax), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r12
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r12,%rax), %r13
	testq	%r13, %r13
	je	.L1419
	cmpb	$0, 56(%r13)
	jne	.L1420
	movq	%r13, %rdi
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	0(%r13), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rcx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rcx, %rax
	je	.L761
	movq	%r13, %rdi
	call	*%rax
.LEHE36:
	movsbl	%al, %esi
	jmp	.L761
.L698:
	movq	%r12, %rdi
.LEHB37:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L699
	movq	%r12, %rdi
	call	*%rax
.LEHE37:
	movsbl	%al, %esi
	jmp	.L699
.L723:
	movq	%r12, %rdi
.LEHB38:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L724
	movq	%r12, %rdi
	call	*%rax
.LEHE38:
	movsbl	%al, %esi
	jmp	.L724
.L673:
	movq	%r12, %rdi
.LEHB39:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L674
	movq	%r12, %rdi
	call	*%rax
.LEHE39:
	movsbl	%al, %esi
	jmp	.L674
	.p2align 4,,10
	.p2align 3
.L762:
	movq	-1288(%rbp), %rax
	vmovq	-1296(%rbp), %xmm6
	movq	-1336(%rbp), %rcx
	vpinsrq	$1, %rax, %xmm6, %xmm5
	movq	%rax, -800(%rbp)
	leaq	-800(%rbp), %rax
	movq	%rcx, -792(%rbp)
	xorl	%edx, %edx
	xorl	%ecx, %ecx
	movq	%rax, %rsi
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.0(%rip), %rdi
	movq	%rax, -1256(%rbp)
	movq	$0, -784(%rbp)
	vmovdqa	%xmm5, -1360(%rbp)
	call	GOMP_parallel@PLT
	movq	-784(%rbp), %r12
	movabsq	$-6148914691236517205, %rdx
	leaq	0(,%r12,4), %rcx
	movq	%rcx, %rax
	mulq	%rdx
	movq	$0, -896(%rbp)
	movq	$0, -888(%rbp)
	movq	%rdx, %rax
	movq	$0, -880(%rbp)
	movq	$0, -864(%rbp)
	shrq	%rax
	movl	$1, %ebx
	cmpq	$5, %rcx
	jbe	.L758
	.p2align 4,,10
	.p2align 3
.L757:
	addq	%rbx, %rbx
	cmpq	%rax, %rbx
	jb	.L757
.L758:
	leaq	-896(%rbp), %rax
	movq	%rbx, %rsi
	movq	%rax, %rdi
	movq	%rax, -1400(%rbp)
.LEHB40:
	call	_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm
.LEHE40:
	decq	%rbx
	movq	%rbx, -872(%rbp)
	call	omp_get_max_threads@PLT
	movl	%eax, -1280(%rbp)
	movslq	%eax, %r13
	movabsq	$384307168202282325, %rax
	cmpq	%rax, %r13
	ja	.L1421
	leaq	0(%r13,%r13,2), %rax
	leaq	0(,%rax,8), %rbx
	movq	$0, -1088(%rbp)
	movq	$0, -1080(%rbp)
	movq	$0, -1072(%rbp)
	movq	%rbx, -1384(%rbp)
	testq	%r13, %r13
	je	.L768
	movq	%rbx, %rdi
.LEHB41:
	call	_Znwm@PLT
.LEHE41:
	vpbroadcastq	%rax, %xmm0
	leaq	(%rax,%rbx), %rsi
	movq	%rax, %rdx
	leaq	-1(%r13), %rax
	movq	%rsi, -1072(%rbp)
	vmovdqa	%xmm0, -1088(%rbp)
	cmpq	$3, %rax
	jbe	.L1070
	movq	%r13, %rcx
	shrq	$2, %rcx
	leaq	(%rcx,%rcx,2), %rcx
	salq	$5, %rcx
	movq	%rdx, %rax
	addq	%rdx, %rcx
	vpxor	%xmm0, %xmm0, %xmm0
.L770:
	vmovdqu	%ymm0, (%rax)
	vmovdqu	%ymm0, 32(%rax)
	vmovdqu	%ymm0, 64(%rax)
	addq	$96, %rax
	cmpq	%rcx, %rax
	jne	.L770
	movq	%r13, %rcx
	andq	$-4, %rcx
	leaq	(%rcx,%rcx,2), %rax
	leaq	(%rdx,%rax,8), %rdx
	movq	%r13, %rax
	subq	%rcx, %rax
	cmpq	%rcx, %r13
	je	.L1422
	vzeroupper
.L769:
	movq	$0, (%rdx)
	movq	$0, 8(%rdx)
	movq	$0, 16(%rdx)
	cmpq	$1, %rax
	je	.L771
	movq	$0, 24(%rdx)
	movq	$0, 32(%rdx)
	movq	$0, 40(%rdx)
	cmpq	$2, %rax
	je	.L771
	movq	$0, 48(%rdx)
	movq	$0, 56(%rdx)
	movq	$0, 64(%rdx)
	cmpq	$3, %rax
	je	.L771
	movq	$0, 72(%rdx)
	movq	$0, 80(%rdx)
	movq	$0, 88(%rdx)
.L771:
	movq	-1336(%rbp), %rax
	movq	%rsi, -1080(%rbp)
	vmovdqa	-1360(%rbp), %xmm6
	movq	-1256(%rbp), %rsi
	movq	%rax, -784(%rbp)
	xorl	%ecx, %ecx
	leaq	-1088(%rbp), %rax
	xorl	%edx, %edx
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.1(%rip), %rdi
	movq	%rax, -1392(%rbp)
	movq	%rax, -776(%rbp)
	vmovdqa	%xmm6, -800(%rbp)
	call	GOMP_parallel@PLT
	movl	-1280(%rbp), %ecx
	testl	%ecx, %ecx
	jle	.L773
	movq	-1088(%rbp), %rax
	leal	-1(%rcx), %edx
	leaq	(%rdx,%rdx,2), %rdx
	leaq	24(%rax), %r11
	movq	-872(%rbp), %rdi
	movq	-896(%rbp), %rsi
	leaq	(%r11,%rdx,8), %rbx
	movabsq	$-7046029254386353131, %r10
	.p2align 4,,10
	.p2align 3
.L775:
	movq	8(%rax), %r8
	movq	(%rax), %rcx
	cmpq	%rcx, %r8
	je	.L781
	.p2align 4,,10
	.p2align 3
.L780:
	movslq	(%rcx), %rax
	movq	%rax, %rdx
	imulq	%r10, %rax
	andq	%rdi, %rax
	leaq	(%rsi,%rax,8), %r9
	cmpb	$0, 4(%r9)
	jne	.L779
	jmp	.L777
	.p2align 4,,10
	.p2align 3
.L1423:
	incq	%rax
	andq	%rdi, %rax
	leaq	(%rsi,%rax,8), %r9
	cmpb	$0, 4(%r9)
	je	.L777
.L779:
	cmpl	(%r9), %edx
	jne	.L1423
.L778:
	addq	$4, %rcx
	cmpq	%r8, %rcx
	jne	.L780
.L781:
	movq	%r11, %rax
	cmpq	%r11, %rbx
	je	.L773
	addq	$24, %r11
	jmp	.L775
	.p2align 4,,10
	.p2align 3
.L777:
	movl	%edx, (%r9)
	movb	$1, 4(%r9)
	incq	-864(%rbp)
	jmp	.L778
.L768:
	movq	-1336(%rbp), %rax
	vmovdqa	-1360(%rbp), %xmm2
	movq	-1256(%rbp), %rsi
	movq	%rax, -784(%rbp)
	xorl	%ecx, %ecx
	leaq	-1088(%rbp), %rax
	xorl	%edx, %edx
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.1(%rip), %rdi
	movq	$0, -1088(%rbp)
	movq	$0, -1072(%rbp)
	movq	$0, -1080(%rbp)
	movq	%rax, -1392(%rbp)
	movq	%rax, -776(%rbp)
	vmovdqa	%xmm2, -800(%rbp)
	call	GOMP_parallel@PLT
.L773:
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	-1312(%rbp), %rax
	vxorpd	%xmm3, %xmm3, %xmm3
	vcvtsi2sdq	%rax, %xmm3, %xmm0
	leaq	.LC28(%rip), %rsi
	movl	$1, %edi
	movl	$1, %eax
	vdivsd	.LC20(%rip), %xmm0, %xmm0
.LEHB42:
	call	__printf_chk@PLT
	movl	$17, %edx
	leaq	.LC29(%rip), %rsi
	movq	%r14, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%r12, %rsi
	movq	%r14, %rdi
	call	_ZNSo9_M_insertIlEERSoT_@PLT
	movl	$5, %edx
	leaq	.LC30(%rip), %rsi
	movq	%rax, %rdi
	movq	%rax, %rbx
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	(%rbx), %rax
	movq	-24(%rax), %rax
	movq	240(%rbx,%rax), %r12
	testq	%r12, %r12
	je	.L1424
	cmpb	$0, 56(%r12)
	je	.L783
	movsbl	67(%r12), %esi
.L784:
	movq	%rbx, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE42:
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	%rax, -1424(%rbp)
	leaq	-704(%rbp), %rax
	movq	%rax, -1320(%rbp)
	movq	%rax, -720(%rbp)
	movq	-1304(%rbp), %rax
	movq	(%rax), %rbx
	movq	8(%rax), %r12
	movq	%rbx, %rax
	addq	%r12, %rax
	je	.L785
	testq	%rbx, %rbx
	je	.L1425
.L785:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1426
	cmpq	$1, %r12
	jne	.L788
	movzbl	(%rbx), %eax
	movb	%al, -704(%rbp)
	movq	-1320(%rbp), %rax
.L789:
	movq	%r12, -712(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-712(%rbp), %rax
	cmpq	$8, %rax
	jbe	.L1427
	leaq	-720(%rbp), %rdi
	movl	$9, %edx
	leaq	.LC31(%rip), %rsi
.LEHB43:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE43:
	movq	-1240(%rbp), %rax
	movq	-720(%rbp), %rbx
	movq	-712(%rbp), %r12
	movq	%rax, -592(%rbp)
	movq	%rbx, %rax
	addq	%r12, %rax
	je	.L795
	testq	%rbx, %rbx
	je	.L1428
.L795:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1429
	cmpq	$1, %r12
	jne	.L798
	movzbl	(%rbx), %eax
	movb	%al, -576(%rbp)
	movq	-1240(%rbp), %rax
.L799:
	movq	%r12, -584(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-584(%rbp), %rax
	cmpq	$14, %rax
	jbe	.L1430
	movl	$15, %edx
	leaq	.LC32(%rip), %rsi
	movq	%r15, %rdi
.LEHB44:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE44:
	leaq	-1056(%rbp), %rax
	movq	%r15, %rsi
	movq	%rax, %rdi
	movq	%rax, -1440(%rbp)
.LEHB45:
	call	_ZN8MmapFileC1ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
.LEHE45:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L805
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L805:
	movq	-1240(%rbp), %rax
	movq	-720(%rbp), %rbx
	movq	-712(%rbp), %r12
	movq	%rax, -592(%rbp)
	movq	%rbx, %rax
	addq	%r12, %rax
	je	.L806
	testq	%rbx, %rbx
	je	.L1431
.L806:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1432
	cmpq	$1, %r12
	jne	.L809
	movzbl	(%rbx), %eax
	movb	%al, -576(%rbp)
	movq	-1240(%rbp), %rax
.L810:
	movq	%r12, -584(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-584(%rbp), %rax
	cmpq	$16, %rax
	jbe	.L1433
	movl	$17, %edx
	leaq	.LC33(%rip), %rsi
	movq	%r15, %rdi
.LEHB46:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE46:
	leaq	-1024(%rbp), %rax
	movq	%r15, %rsi
	movq	%rax, %rdi
	movq	%rax, -1448(%rbp)
.LEHB47:
	call	_ZN8MmapFileC1ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
.LEHE47:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L816
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L816:
	movq	-1240(%rbp), %rax
	movq	-720(%rbp), %rbx
	movq	-712(%rbp), %r12
	movq	%rax, -592(%rbp)
	movq	%rbx, %rax
	addq	%r12, %rax
	je	.L817
	testq	%rbx, %rbx
	je	.L1434
.L817:
	movq	%r12, -800(%rbp)
	cmpq	$15, %r12
	ja	.L1435
	cmpq	$1, %r12
	jne	.L820
	movzbl	(%rbx), %eax
	movb	%al, -576(%rbp)
	movq	-1240(%rbp), %rax
.L821:
	movq	%r12, -584(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-584(%rbp), %rax
	cmpq	$17, %rax
	jbe	.L1436
	movl	$18, %edx
	leaq	.LC34(%rip), %rsi
	movq	%r15, %rdi
.LEHB48:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE48:
	leaq	-992(%rbp), %rax
	movq	%r15, %rsi
	movq	%rax, %rdi
	movq	%rax, -1456(%rbp)
.LEHB49:
	call	_ZN8MmapFileC1ERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
.LEHE49:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L827
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L827:
	movq	-1056(%rbp), %rax
	movq	%rax, -1328(%rbp)
	testq	%rax, %rax
	je	.L828
	movq	-1024(%rbp), %rax
	movq	%rax, -1304(%rbp)
	testq	%rax, %rax
	je	.L828
	movq	-992(%rbp), %rax
	movq	%rax, -1312(%rbp)
	testq	%rax, %rax
	je	.L828
	movq	-1048(%rbp), %rax
	movq	%rax, -1416(%rbp)
	shrq	$2, %rax
	movq	%rax, %rbx
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	-1424(%rbp), %rax
	vxorpd	%xmm4, %xmm4, %xmm4
	vcvtsi2sdq	%rax, %xmm4, %xmm0
	leaq	.LC36(%rip), %rsi
	movl	$1, %edi
	movl	$1, %eax
	vdivsd	.LC20(%rip), %xmm0, %xmm0
.LEHB50:
	call	__printf_chk@PLT
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	%rax, %r12
	movq	$0, -960(%rbp)
	movq	$0, -952(%rbp)
	movq	$0, -944(%rbp)
	testq	%r13, %r13
	je	.L834
	movq	-1384(%rbp), %rdi
	call	_Znwm@PLT
.LEHE50:
	movq	-1384(%rbp), %rsi
	vpbroadcastq	%rax, %xmm0
	addq	%rax, %rsi
	leaq	-1(%r13), %rdx
	movq	%rsi, -944(%rbp)
	vmovdqa	%xmm0, -960(%rbp)
	cmpq	$3, %rdx
	jbe	.L1077
	movq	%r13, %rcx
	shrq	$2, %rcx
	leaq	(%rcx,%rcx,2), %rcx
	salq	$5, %rcx
	movq	%rax, %rdx
	addq	%rax, %rcx
	vpxor	%xmm0, %xmm0, %xmm0
.L836:
	vmovdqu	%ymm0, (%rdx)
	vmovdqu	%ymm0, 32(%rdx)
	vmovdqu	%ymm0, 64(%rdx)
	addq	$96, %rdx
	cmpq	%rdx, %rcx
	jne	.L836
	movq	%r13, %rcx
	andq	$-4, %rcx
	leaq	(%rcx,%rcx,2), %rdx
	leaq	(%rax,%rdx,8), %rax
	movq	%r13, %rdx
	subq	%rcx, %rdx
	cmpq	%rcx, %r13
	je	.L1437
	vzeroupper
.L835:
	movq	$0, (%rax)
	movq	$0, 8(%rax)
	movq	$0, 16(%rax)
	cmpq	$1, %rdx
	je	.L837
	movq	$0, 24(%rax)
	movq	$0, 32(%rax)
	movq	$0, 40(%rax)
	cmpq	$2, %rdx
	je	.L837
	movq	$0, 48(%rax)
	movq	$0, 56(%rax)
	movq	$0, 64(%rax)
	cmpq	$3, %rdx
	je	.L837
	movq	$0, 72(%rax)
	movq	$0, 80(%rax)
	movq	$0, 88(%rax)
.L837:
	movq	-1312(%rbp), %rax
	movq	%rsi, -952(%rbp)
	movq	%rax, -776(%rbp)
	movq	-1304(%rbp), %rax
	movq	-1256(%rbp), %rsi
	movq	%rax, -784(%rbp)
	movq	-1328(%rbp), %rax
	xorl	%ecx, %ecx
	movq	%rax, -792(%rbp)
	movq	-1400(%rbp), %rax
	xorl	%edx, %edx
	movq	%rax, -800(%rbp)
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.2(%rip), %rdi
	leaq	-960(%rbp), %rax
	movq	%rax, -1400(%rbp)
	movq	%rax, -760(%rbp)
	movq	%rbx, -768(%rbp)
	call	GOMP_parallel@PLT
	movl	-1280(%rbp), %eax
	testl	%eax, %eax
	jle	.L838
	leal	-1(%rax), %ecx
	movq	-960(%rbp), %rsi
	cmpl	$3, %ecx
	jbe	.L1078
	movl	%ecx, %edx
	shrl	$2, %edx
	decl	%edx
	leaq	(%rdx,%rdx,2), %rdx
	salq	$5, %rdx
	vpxor	%xmm2, %xmm2, %xmm2
	vmovdqa	.LC37(%rip), %ymm10
	vmovdqa	.LC38(%rip), %ymm9
	vmovdqa	.LC39(%rip), %ymm8
	vmovdqa	.LC40(%rip), %ymm7
	vpbroadcastq	.LC55(%rip), %ymm6
	movq	%rsi, %rax
	leaq	96(%rsi,%rdx), %rdx
	vmovdqa	%ymm2, %ymm5
.L840:
	vmovdqu	(%rax), %ymm0
	vmovdqu	32(%rax), %ymm4
	vmovdqu	64(%rax), %ymm3
	vmovdqa	%ymm0, %ymm1
	vpermt2q	%ymm4, %ymm10, %ymm1
	vpermt2q	%ymm4, %ymm8, %ymm0
	vpermt2q	%ymm3, %ymm7, %ymm0
	vpermt2q	%ymm3, %ymm9, %ymm1
	vpsubq	%ymm0, %ymm1, %ymm1
	vpcmpgtq	%ymm1, %ymm5, %ymm0
	addq	$96, %rax
	vpand	%ymm6, %ymm0, %ymm0
	vpaddq	%ymm1, %ymm0, %ymm0
	vpsraq	$2, %ymm0, %ymm0
	vpaddq	%ymm2, %ymm0, %ymm2
	cmpq	%rax, %rdx
	jne	.L840
	vextracti64x2	$0x1, %ymm2, %xmm0
	vpaddq	%xmm2, %xmm0, %xmm0
	vpsrldq	$8, %xmm0, %xmm1
	movl	%ecx, %eax
	vpaddq	%xmm1, %xmm0, %xmm0
	andl	$-4, %eax
	vmovq	%xmm0, %rdx
	movl	%eax, %edi
	vzeroupper
.L839:
	subl	%eax, %ecx
	cmpl	$1, %ecx
	jbe	.L841
	leaq	(%rax,%rax,2), %rax
	leaq	(%rsi,%rax,8), %rax
	vmovdqu	(%rax), %xmm0
	andl	$-2, %ecx
	vpalignr	$8, %xmm0, %xmm0, %xmm1
	vpunpcklqdq	32(%rax), %xmm1, %xmm1
	vshufpd	$2, 16(%rax), %xmm0, %xmm0
	vpsubq	%xmm0, %xmm1, %xmm1
	vpxor	%xmm0, %xmm0, %xmm0
	vpcmpgtq	%xmm1, %xmm0, %xmm0
	addl	%ecx, %edi
	vpandq	.LC55(%rip){1to2}, %xmm0, %xmm0
	vpaddq	%xmm1, %xmm0, %xmm0
	vpsraq	$2, %xmm0, %xmm0
	vpsrldq	$8, %xmm0, %xmm1
	vpaddq	%xmm1, %xmm0, %xmm0
	vmovq	%xmm0, %rax
	addq	%rax, %rdx
.L841:
	movslq	%edi, %rax
	leaq	(%rax,%rax,2), %rcx
	salq	$3, %rcx
	leaq	(%rsi,%rcx), %r8
	movq	8(%r8), %rax
	incl	%edi
	subq	(%r8), %rax
	sarq	$2, %rax
	addq	%rdx, %rax
	cmpl	%edi, -1280(%rbp)
	jle	.L842
	leaq	24(%rsi,%rcx), %rcx
	movq	8(%rcx), %rdx
	subq	(%rcx), %rdx
	sarq	$2, %rdx
	addq	%rdx, %rax
.L842:
	leaq	0(,%rax,4), %rcx
	movq	%rcx, %rax
	movabsq	$-6148914691236517205, %rdx
	mulq	%rdx
	movl	$1, %ebx
	movq	%rdx, %rax
	shrq	%rax
	cmpq	$5, %rcx
	jbe	.L843
.L844:
	addq	%rbx, %rbx
	cmpq	%rbx, %rax
	ja	.L844
.L843:
	leaq	-848(%rbp), %rax
	movq	%rbx, %rsi
	movq	%rax, %rdi
	movq	$0, -848(%rbp)
	movq	$0, -840(%rbp)
	movq	$0, -832(%rbp)
	movq	$0, -816(%rbp)
	movq	%rax, -1384(%rbp)
.LEHB51:
	call	_ZNSt6vectorIN14CompactHashSetIiE5EntryESaIS2_EE17_M_default_appendEm
.LEHE51:
	movl	-1280(%rbp), %edx
	decq	%rbx
	movq	%rbx, -824(%rbp)
	testl	%edx, %edx
	jle	.L852
	movl	-1280(%rbp), %ecx
	movq	-960(%rbp), %rax
	leal	-1(%rcx), %edx
	leaq	(%rdx,%rdx,2), %rdx
	leaq	24(%rax), %r10
	movq	-848(%rbp), %rsi
	leaq	(%r10,%rdx,8), %r11
	movabsq	$-7046029254386353131, %r9
.L851:
	movq	8(%rax), %rdi
	movq	(%rax), %rcx
	cmpq	%rcx, %rdi
	je	.L857
	.p2align 4,,10
	.p2align 3
.L856:
	movslq	(%rcx), %rax
	movq	%rax, %rdx
	imulq	%r9, %rax
	andq	%rbx, %rax
	leaq	(%rsi,%rax,8), %r8
	cmpb	$0, 4(%r8)
	jne	.L855
	jmp	.L853
	.p2align 4,,10
	.p2align 3
.L1438:
	incq	%rax
	andq	%rbx, %rax
	leaq	(%rsi,%rax,8), %r8
	cmpb	$0, 4(%r8)
	je	.L853
.L855:
	cmpl	(%r8), %edx
	jne	.L1438
.L854:
	addq	$4, %rcx
	cmpq	%rdi, %rcx
	jne	.L856
.L857:
	movq	%r10, %rax
	cmpq	%r11, %r10
	je	.L852
	addq	$24, %r10
	jmp	.L851
	.p2align 4,,10
	.p2align 3
.L853:
	movl	%edx, (%r8)
	movb	$1, 4(%r8)
	incq	-816(%rbp)
	jmp	.L854
.L852:
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	%r12, %rax
	vxorpd	%xmm5, %xmm5, %xmm5
	vcvtsi2sdq	%rax, %xmm5, %xmm0
	leaq	.LC43(%rip), %rsi
	movl	$1, %edi
	movl	$1, %eax
	vdivsd	.LC20(%rip), %xmm0, %xmm0
.LEHB52:
	call	__printf_chk@PLT
	movl	$20, %edx
	leaq	.LC44(%rip), %rsi
	movq	%r14, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-816(%rbp), %rsi
	movq	%r14, %rdi
	call	_ZNSo9_M_insertImEERSoT_@PLT
	movl	$20, %edx
	leaq	.LC45(%rip), %rsi
	movq	%rax, %rdi
	movq	%rax, %rbx
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	(%rbx), %rax
	movq	-24(%rax), %rax
	movq	240(%rbx,%rax), %r12
	testq	%r12, %r12
	je	.L1439
	cmpb	$0, 56(%r12)
	je	.L859
	movsbl	67(%r12), %esi
.L860:
	movq	%rbx, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	%rax, %r12
	leaq	0(%r13,%r13,4), %rax
	movq	$0, -928(%rbp)
	movq	$0, -920(%rbp)
	movq	$0, -912(%rbp)
	leaq	0(,%rax,8), %rbx
	testq	%r13, %r13
	je	.L861
	movq	%rbx, %rdi
	call	_Znwm@PLT
.LEHE52:
	vpbroadcastq	%rax, %xmm0
	vmovdqa	%xmm0, -928(%rbp)
	leaq	(%rax,%rbx), %rsi
	vpxor	%xmm0, %xmm0, %xmm0
	movq	%rsi, -912(%rbp)
	movq	$0, 32(%rax)
	movq	%rax, %rdx
	vmovdqu	%ymm0, (%rax)
	leaq	40(%rax), %rcx
	cmpq	$1, %r13
	je	.L862
	cmpq	%rcx, %rsi
	je	.L863
	movq	%rcx, %rax
.L864:
	vmovdqu	(%rdx), %xmm5
	addq	$40, %rax
	vmovdqu	%xmm5, -40(%rax)
	vmovdqu	16(%rdx), %xmm6
	vmovdqu	%xmm6, -24(%rax)
	movq	32(%rdx), %rdi
	movq	%rdi, -8(%rax)
	cmpq	%rax, %rsi
	jne	.L864
	leaq	-80(%rbx), %rax
	movabsq	$922337203685477581, %rsi
	shrq	$3, %rax
	imulq	%rsi, %rax
	movabsq	$2305843009213693951, %rsi
	andq	%rsi, %rax
	leaq	5(%rax,%rax,4), %rax
	leaq	(%rcx,%rax,8), %rcx
.L863:
	movq	%rcx, -920(%rbp)
	movl	-1280(%rbp), %ecx
	testl	%ecx, %ecx
	jle	.L1366
	vmovq	-1384(%rbp), %xmm5
	leaq	-928(%rbp), %rax
	vpinsrq	$1, %rax, %xmm5, %xmm1
	movl	%ecx, %ebx
	cmpl	$4, %ecx
	jle	.L1081
	shrl	$2, %ecx
	leaq	(%rcx,%rcx,4), %rcx
	salq	$5, %rcx
	movq	%rdx, %rax
	addq	%rdx, %rcx
	vpxor	%xmm0, %xmm0, %xmm0
.L867:
	vmovdqu	%ymm0, (%rax)
	vmovdqu	%ymm0, 32(%rax)
	vmovdqu	%ymm0, 64(%rax)
	vmovdqu	%ymm0, 96(%rax)
	vmovdqu	%ymm0, 128(%rax)
	addq	$160, %rax
	cmpq	%rcx, %rax
	jne	.L867
	movl	-1280(%rbp), %ecx
	movl	%ecx, %eax
	andl	$-4, %eax
	andl	$3, %ecx
	je	.L868
.L866:
	movslq	%eax, %rcx
	leaq	(%rcx,%rcx,4), %rcx
	salq	$3, %rcx
	leaq	(%rdx,%rcx), %rsi
	vpxor	%xmm0, %xmm0, %xmm0
	movl	-1280(%rbp), %edi
	movq	$0, 32(%rsi)
	vmovdqu	%ymm0, (%rsi)
	leal	1(%rax), %esi
	cmpl	%esi, %edi
	jle	.L869
	leaq	40(%rdx,%rcx), %rsi
	movq	$0, 32(%rsi)
	vmovdqu	%ymm0, (%rsi)
	leal	2(%rax), %esi
	cmpl	%esi, %edi
	jle	.L869
	leaq	80(%rdx,%rcx), %rsi
	addl	$3, %eax
	movq	$0, 32(%rsi)
	vmovdqu	%ymm0, (%rsi)
	cmpl	%eax, %edi
	jle	.L869
	leaq	120(%rdx,%rcx), %rax
	movq	$0, 32(%rax)
	vmovdqu	%ymm0, (%rax)
.L868:
	movq	-1336(%rbp), %rax
	vmovdqa	-1360(%rbp), %xmm3
	movq	%rax, -776(%rbp)
	movq	-1256(%rbp), %rsi
	movq	-1264(%rbp), %rax
	xorl	%ecx, %ecx
	xorl	%edx, %edx
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3(%rip), %rdi
	movq	%rax, -784(%rbp)
	vmovdqa	%xmm3, -800(%rbp)
	vmovdqa	%xmm1, -768(%rbp)
	vzeroupper
	call	GOMP_parallel@PLT
	movq	$0, -624(%rbp)
	vpxor	%xmm0, %xmm0, %xmm0
	movq	-928(%rbp), %rdx
	vmovdqa	%ymm0, -656(%rbp)
.L1049:
	movl	%ebx, %ecx
	shrl	$2, %ecx
	decl	%ecx
	leaq	(%rcx,%rcx,4), %rcx
	vpxor	%xmm2, %xmm2, %xmm2
	salq	$5, %rcx
	movq	%rdx, %rax
	leaq	160(%rdx,%rcx), %rcx
	vmovdqa	%ymm2, %ymm3
	vmovdqa	%ymm2, %ymm0
	vmovdqa	%ymm2, %ymm1
	vmovdqa	%ymm2, %ymm4
.L873:
	vpaddq	(%rax), %ymm4, %ymm4
	vpaddq	32(%rax), %ymm1, %ymm1
	vpaddq	64(%rax), %ymm0, %ymm0
	vpaddq	96(%rax), %ymm3, %ymm3
	vpaddq	128(%rax), %ymm2, %ymm2
	addq	$160, %rax
	cmpq	%rax, %rcx
	jne	.L873
	vmovq	%xmm4, %rax
	vpextrq	$1, %xmm1, %r8
	vextracti64x2	$0x1, %ymm0, %xmm7
	addq	%rax, %r8
	vextracti64x2	$0x1, %ymm3, %xmm5
	vmovq	%xmm7, %rax
	vmovdqa	%xmm1, %xmm6
	addq	%rax, %r8
	vextracti64x2	$0x1, %ymm1, %xmm1
	vpextrq	$1, %xmm5, %rax
	addq	%rax, %r8
	vmovq	%xmm1, %rdi
	vpextrq	$1, %xmm4, %rax
	addq	%rax, %rdi
	vpextrq	$1, %xmm7, %rax
	addq	%rax, %rdi
	vextracti64x2	$0x1, %ymm4, %xmm4
	vmovq	%xmm2, %rax
	addq	%rax, %rdi
	vpextrq	$1, %xmm1, %rcx
	vmovq	%xmm4, %rax
	addq	%rax, %rcx
	vmovq	%xmm3, %rax
	addq	%rax, %rcx
	vpextrq	$1, %xmm2, %rax
	addq	%rax, %rcx
	vmovq	%xmm0, %rsi
	vpextrq	$1, %xmm4, %rax
	addq	%rax, %rsi
	vextracti64x2	$0x1, %ymm2, %xmm2
	vpextrq	$1, %xmm3, %rax
	addq	%rax, %rsi
	vmovq	%xmm2, %rax
	vmovq	%xmm6, %r9
	addq	%rax, %rsi
	vpextrq	$1, %xmm0, %rax
	addq	%r9, %rax
	movl	%ebx, %r10d
	vmovq	%xmm5, %r9
	addq	%r9, %rax
	andl	$-4, %r10d
	vpextrq	$1, %xmm2, %r9
	addq	%r9, %rax
	movl	%r10d, %r9d
	cmpl	%r10d, %ebx
	je	.L870
.L1047:
	subl	%r10d, %ebx
	cmpl	$1, %ebx
	je	.L871
	vmovq	%r8, %xmm6
	leaq	(%r10,%r10,4), %r10
	leaq	(%rdx,%r10,8), %r10
	vpinsrq	$1, %rdi, %xmm6, %xmm2
	vmovq	%rax, %xmm0
	vpaddq	(%r10), %xmm2, %xmm2
	vpaddq	32(%r10), %xmm0, %xmm0
	vmovq	%rcx, %xmm7
	vmovdqu	48(%r10), %xmm4
	vpinsrq	$1, %rsi, %xmm7, %xmm1
	vpaddq	16(%r10), %xmm1, %xmm1
	vmovq	%xmm2, %rax
	vpextrq	$1, %xmm0, %r8
	vmovdqu	64(%r10), %xmm3
	addq	%rax, %r8
	vpextrq	$1, %xmm2, %rdi
	vmovq	%xmm4, %rax
	addq	%rax, %rdi
	vmovq	%xmm1, %rcx
	vpextrq	$1, %xmm4, %rax
	addq	%rax, %rcx
	vpextrq	$1, %xmm1, %rsi
	vmovq	%xmm3, %rax
	vpextrq	$1, %xmm3, %r10
	addq	%rax, %rsi
	vmovq	%xmm0, %rax
	addq	%r10, %rax
	movl	%ebx, %r10d
	andl	$-2, %r10d
	addl	%r10d, %r9d
	cmpl	%ebx, %r10d
	je	.L870
.L871:
	movslq	%r9d, %r9
	leaq	(%r9,%r9,4), %r9
	leaq	(%rdx,%r9,8), %rdx
	addq	(%rdx), %r8
	addq	8(%rdx), %rdi
	addq	16(%rdx), %rcx
	addq	24(%rdx), %rsi
	addq	32(%rdx), %rax
.L870:
	movq	%r8, -656(%rbp)
	movq	%rdi, -648(%rbp)
	movq	%rcx, -640(%rbp)
	movq	%rsi, -632(%rbp)
	movq	%rax, -624(%rbp)
.L1048:
	vzeroupper
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	%r12, %rax
	vxorpd	%xmm6, %xmm6, %xmm6
	vcvtsi2sdq	%rax, %xmm6, %xmm0
	leaq	.LC46(%rip), %rsi
	movl	$1, %edi
	movl	$1, %eax
	vdivsd	.LC20(%rip), %xmm0, %xmm0
.LEHB53:
	call	__printf_chk@PLT
.LEHE53:
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	movq	%rax, -1384(%rbp)
	leaq	-1224(%rbp), %rax
	movq	$0, -800(%rbp)
	movq	$0, -792(%rbp)
	movq	$0, -784(%rbp)
	movq	$0, -1280(%rbp)
	movq	%rax, -1424(%rbp)
	xorl	%r13d, %r13d
	xorl	%ebx, %ebx
.L875:
	movq	-1120(%rbp), %rdx
	movq	-1112(%rbp), %rax
	subq	%rdx, %rax
	sarq	$5, %rax
	cmpq	%rbx, %rax
	jbe	.L886
	movq	-1248(%rbp), %rax
	movq	(%rax,%rbx,8), %rax
	movq	%rax, -1336(%rbp)
	testq	%rax, %rax
	jg	.L1440
.L888:
	incq	%rbx
	cmpq	$5, %rbx
	jne	.L875
.L886:
	movq	-800(%rbp), %rcx
	movq	%rcx, -1240(%rbp)
	movq	%rcx, %r12
	cmpq	%r13, %rcx
	je	.L889
	movq	%r13, %rbx
	subq	%rcx, %rbx
	movq	%rbx, %rax
	sarq	$3, %rax
	movabsq	$-3689348814741910323, %rdx
	imulq	%rdx, %rax
	movl	$63, %edx
	movq	%r13, %rsi
	lzcntq	%rax, %rax
	subl	%eax, %edx
	movslq	%edx, %rdx
	addq	%rdx, %rdx
	movq	%rcx, %rdi
	call	_ZSt16__introsort_loopIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEElNS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_T1_
	cmpq	$640, %rbx
	jle	.L890
	leaq	640(%r12), %rbx
	movq	%rbx, %rsi
	movq	%r12, %rdi
	call	_ZSt16__insertion_sortIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_.constprop.0
	cmpq	%r13, %rbx
	je	.L889
.L893:
	movq	%rbx, %rdi
	addq	$40, %rbx
	call	_ZSt25__unguarded_linear_insertIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops14_Val_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SM_.constprop.0
	cmpq	%rbx, %r13
	jne	.L893
.L889:
	leaq	-672(%rbp), %rax
	movq	%rax, -1248(%rbp)
	movq	%rax, -688(%rbp)
	movq	-1344(%rbp), %rax
	movq	(%rax), %rbx
	movq	8(%rax), %r12
	movq	%rbx, %rax
	addq	%r12, %rax
	je	.L895
	testq	%rbx, %rbx
	je	.L1441
.L895:
	movq	%r12, -1224(%rbp)
	cmpq	$15, %r12
	ja	.L1442
	cmpq	$1, %r12
	jne	.L898
	movzbl	(%rbx), %eax
	movb	%al, -672(%rbp)
	movq	-1248(%rbp), %rax
.L899:
	movq	%r12, -680(%rbp)
	movb	$0, (%rax,%r12)
	movabsq	$4611686018427387903, %rax
	subq	-680(%rbp), %rax
	cmpq	$6, %rax
	jbe	.L1443
	leaq	-688(%rbp), %rdi
	movl	$7, %edx
	leaq	.LC47(%rip), %rsi
.LEHB54:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_appendEPKcm@PLT
.LEHE54:
	leaq	-344(%rbp), %r12
	movq	%r12, %rdi
	movq	%r12, -1336(%rbp)
	call	_ZNSt8ios_baseC2Ev@PLT
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -344(%rbp)
	xorl	%eax, %eax
	movw	%ax, -120(%rbp)
	vpxor	%xmm0, %xmm0, %xmm0
	movq	8+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	vmovdqa	%ymm0, -112(%rbp)
	movq	-24(%rax), %rdi
	movq	%rax, -592(%rbp)
	movq	16+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	addq	%r15, %rdi
	xorl	%esi, %esi
	movq	$0, -128(%rbp)
	movq	%rax, (%rdi)
	vzeroupper
.LEHB55:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E@PLT
.LEHE55:
	leaq	24+_ZTVSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -592(%rbp)
	addq	$40, %rax
	movq	%rax, -344(%rbp)
	leaq	-584(%rbp), %rax
	movq	%rax, %rdi
	movq	%rax, -1256(%rbp)
	movq	%rax, %rbx
.LEHB56:
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEEC1Ev@PLT
.LEHE56:
	movq	%rbx, %rsi
	movq	%r12, %rdi
.LEHB57:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E@PLT
	movq	-688(%rbp), %rsi
	movl	$16, %edx
	movq	%rbx, %rdi
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE4openEPKcSt13_Ios_Openmode@PLT
	movq	-592(%rbp), %rdx
	movq	-24(%rdx), %rdi
	addq	%r15, %rdi
	testq	%rax, %rax
	je	.L1444
	xorl	%esi, %esi
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate@PLT
.LEHE57:
.L906:
	leaq	-480(%rbp), %rax
	movq	%rax, %rdi
	movq	%rax, -1360(%rbp)
	call	_ZNKSt12__basic_fileIcE7is_openEv@PLT
	testb	%al, %al
	je	.L1445
	movl	$28, %edx
	leaq	.LC49(%rip), %rsi
	movq	%r15, %rdi
.LEHB58:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-1240(%rbp), %rax
	movq	%rax, %rbx
	cmpq	%r13, %rax
	je	.L938
.L937:
	movq	8(%rbx), %rdx
	movq	(%rbx), %rsi
	movq	%r15, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movl	$1, %edx
	leaq	.LC50(%rip), %rsi
	movq	%rax, %rdi
	movq	%rax, %r12
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	32(%rbx), %rsi
	movq	%r12, %rdi
	call	_ZNSo9_M_insertIlEERSoT_@PLT
	movq	%rax, %rdi
	movl	$1, %edx
	leaq	.LC51(%rip), %rsi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	addq	$40, %rbx
	cmpq	%rbx, %r13
	jne	.L937
.L938:
	movq	-1256(%rbp), %rdi
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE5closeEv@PLT
	testq	%rax, %rax
	je	.L1446
.L939:
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	-1384(%rbp), %rax
	vxorpd	%xmm7, %xmm7, %xmm7
	vcvtsi2sdq	%rax, %xmm7, %xmm0
	leaq	.LC52(%rip), %rsi
	movl	$1, %edi
	movl	$1, %eax
	vdivsd	.LC20(%rip), %xmm0, %xmm2
	vmovsd	%xmm2, %xmm2, %xmm0
	vmovq	%xmm2, %rbx
	call	__printf_chk@PLT
	call	_ZNSt6chrono3_V212system_clock3nowEv@PLT
	subq	-1376(%rbp), %rax
	vxorpd	%xmm7, %xmm7, %xmm7
	vcvtsi2sdq	%rax, %xmm7, %xmm0
	vmovq	%rbx, %xmm3
	leaq	.LC53(%rip), %rsi
	movl	$1, %edi
	vdivsd	.LC20(%rip), %xmm0, %xmm0
	vsubsd	%xmm3, %xmm0, %xmm0
	movl	$1, %eax
	call	__printf_chk@PLT
	movl	$22, %edx
	leaq	.LC54(%rip), %rsi
	movq	%r14, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-680(%rbp), %rdx
	movq	-688(%rbp), %rsi
	movq	%r14, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r14
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r14,%rax), %r12
	testq	%r12, %r12
	je	.L1447
	cmpb	$0, 56(%r12)
	je	.L941
	movsbl	67(%r12), %esi
.L942:
	movq	%r14, %rdi
	call	_ZNSo3putEc@PLT
	vmovq	.LC56(%rip), %xmm5
	movq	%rax, %rdi
	leaq	16+_ZTVSt13basic_filebufIcSt11char_traitsIcEE(%rip), %rax
	vpinsrq	$1, %rax, %xmm5, %xmm7
	vmovdqa	%xmm7, -1376(%rbp)
	call	_ZNSo5flushEv@PLT
.LEHE58:
	vmovdqa	-1376(%rbp), %xmm7
	movq	-1256(%rbp), %rdi
	leaq	64+_ZTVSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -344(%rbp)
	vmovdqa	%xmm7, -592(%rbp)
.LEHB59:
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE5closeEv@PLT
.LEHE59:
.L946:
	movq	-1360(%rbp), %rdi
	call	_ZNSt12__basic_fileIcED1Ev@PLT
	leaq	16+_ZTVSt15basic_streambufIcSt11char_traitsIcEE(%rip), %rax
	leaq	-528(%rbp), %rdi
	movq	%rax, -584(%rbp)
	call	_ZNSt6localeD1Ev@PLT
	movq	8+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	16+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rcx
	movq	%rax, -592(%rbp)
	movq	-24(%rax), %rax
	movq	-1336(%rbp), %rdi
	movq	%rcx, -592(%rbp,%rax)
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -344(%rbp)
	call	_ZNSt8ios_baseD2Ev@PLT
	movq	-688(%rbp), %rdi
	cmpq	-1248(%rbp), %rdi
	je	.L944
	movq	-672(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L944:
	movq	-1240(%rbp), %rax
	movq	%rax, %rbx
	cmpq	%r13, %rax
	je	.L952
.L947:
	movq	(%rbx), %rdi
	leaq	16(%rbx), %rax
	cmpq	%rax, %rdi
	je	.L950
	movq	16(%rbx), %rax
	addq	$40, %rbx
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	cmpq	%r13, %rbx
	jne	.L947
.L952:
	cmpq	$0, -1240(%rbp)
	je	.L949
	movq	-1280(%rbp), %rsi
	movq	-1240(%rbp), %rdi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L949:
	movq	-928(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L953
	movq	-912(%rbp), %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L953:
	movq	-848(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L954
	movq	-832(%rbp), %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L954:
	movq	-952(%rbp), %rbx
	movq	-960(%rbp), %r12
	cmpq	%r12, %rbx
	je	.L955
.L959:
	movq	(%r12), %rdi
	testq	%rdi, %rdi
	je	.L956
	movq	16(%r12), %rsi
	addq	$24, %r12
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
	cmpq	%r12, %rbx
	jne	.L959
.L958:
	movq	-960(%rbp), %r12
.L955:
	testq	%r12, %r12
	je	.L960
	movq	-944(%rbp), %rsi
	movq	%r12, %rdi
	subq	%r12, %rsi
	call	_ZdlPvm@PLT
.L960:
	cmpq	$-1, -1312(%rbp)
	jne	.L1448
.L961:
	movl	-976(%rbp), %edi
	testl	%edi, %edi
	js	.L962
	call	close@PLT
.L962:
	cmpq	$-1, -1304(%rbp)
	jne	.L1449
.L963:
	movl	-1008(%rbp), %edi
	testl	%edi, %edi
	js	.L964
	call	close@PLT
.L964:
	cmpq	$-1, -1328(%rbp)
	jne	.L1450
.L965:
	movl	-1040(%rbp), %edi
	testl	%edi, %edi
	js	.L966
	call	close@PLT
.L966:
	movq	-720(%rbp), %rdi
	cmpq	-1320(%rbp), %rdi
	je	.L967
	movq	-704(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L967:
	movq	-1080(%rbp), %rbx
	movq	-1088(%rbp), %r12
	cmpq	%r12, %rbx
	je	.L968
.L972:
	movq	(%r12), %rdi
	testq	%rdi, %rdi
	je	.L969
	movq	16(%r12), %rsi
	addq	$24, %r12
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
	cmpq	%rbx, %r12
	jne	.L972
.L971:
	movq	-1088(%rbp), %r12
.L968:
	testq	%r12, %r12
	je	.L973
	movq	-1072(%rbp), %rsi
	movq	%r12, %rdi
	subq	%r12, %rsi
	call	_ZdlPvm@PLT
.L973:
	movq	-896(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L974
	movq	-880(%rbp), %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L974:
	movq	-1112(%rbp), %rbx
	movq	-1120(%rbp), %r12
	cmpq	%r12, %rbx
	je	.L975
.L979:
	movq	(%r12), %rdi
	leaq	16(%r12), %rax
	cmpq	%rax, %rdi
	je	.L976
	movq	16(%r12), %rax
	addq	$32, %r12
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	cmpq	%rbx, %r12
	jne	.L979
.L978:
	movq	-1120(%rbp), %r12
.L975:
	testq	%r12, %r12
	je	.L980
	movq	-1104(%rbp), %rsi
	movq	%r12, %rdi
	subq	%r12, %rsi
	call	_ZdlPvm@PLT
.L980:
	cmpq	$-1, -1264(%rbp)
	jne	.L1451
.L981:
	movl	-1136(%rbp), %edi
	testl	%edi, %edi
	js	.L982
	call	close@PLT
.L982:
	cmpq	$-1, -1288(%rbp)
	jne	.L1452
.L983:
	movl	-1168(%rbp), %edi
	testl	%edi, %edi
	js	.L984
	call	close@PLT
.L984:
	cmpq	$-1, -1296(%rbp)
	jne	.L1453
.L985:
	movl	-1200(%rbp), %edi
	testl	%edi, %edi
	js	.L1014
	call	close@PLT
	jmp	.L1014
.L976:
	addq	$32, %r12
	cmpq	%r12, %rbx
	jne	.L979
	jmp	.L978
.L956:
	addq	$24, %r12
	cmpq	%r12, %rbx
	jne	.L959
	jmp	.L958
.L969:
	addq	$24, %r12
	cmpq	%r12, %rbx
	jne	.L972
	jmp	.L971
.L950:
	addq	$40, %rbx
	cmpq	%r13, %rbx
	jne	.L947
	jmp	.L952
.L1440:
	movq	%rbx, %rax
	salq	$5, %rax
	addq	%rax, %rdx
	movq	-1240(%rbp), %rax
	movq	8(%rdx), %r12
	movq	%rax, -592(%rbp)
	movq	(%rdx), %rax
	movq	%rax, -1360(%rbp)
	movq	%rax, %rcx
	addq	%r12, %rax
	je	.L876
	testq	%rcx, %rcx
	je	.L1454
.L876:
	movq	%r12, -1224(%rbp)
	cmpq	$15, %r12
	ja	.L1455
	cmpq	$1, %r12
	jne	.L879
	movq	-1360(%rbp), %rax
	movzbl	(%rax), %eax
	movb	%al, -576(%rbp)
	movq	-1240(%rbp), %rax
.L880:
	movq	%r12, -584(%rbp)
	movb	$0, (%rax,%r12)
	movq	-1336(%rbp), %rax
	movq	%rax, -560(%rbp)
	cmpq	-1280(%rbp), %r13
	je	.L881
	leaq	16(%r13), %rax
	movq	%rax, 0(%r13)
	movq	-592(%rbp), %rax
	cmpq	-1240(%rbp), %rax
	je	.L1456
	movq	%rax, 0(%r13)
	movq	-576(%rbp), %rax
	movq	%rax, 16(%r13)
.L883:
	movq	-584(%rbp), %rax
	addq	$40, %r13
	movq	%rax, -32(%r13)
	movq	-560(%rbp), %rax
	movq	%rax, -8(%r13)
	movq	%r13, -792(%rbp)
	jmp	.L888
.L828:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$29, %edx
	leaq	.LC35(%rip), %rsi
	movq	%r12, %rdi
.LEHB60:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	_ZSt4cerr(%rip), %rax
	movq	-24(%rax), %rax
	movq	240(%r12,%rax), %r13
	testq	%r13, %r13
	je	.L1457
	cmpb	$0, 56(%r13)
	je	.L831
	movsbl	67(%r13), %esi
.L832:
	movq	%r12, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
.LEHE60:
	movq	-1024(%rbp), %rax
	movq	%rax, -1304(%rbp)
	movq	-992(%rbp), %rax
	movq	%rax, -1312(%rbp)
.L934:
	movq	-1312(%rbp), %rax
	decq	%rax
	cmpq	$-3, %rax
	jbe	.L1458
.L989:
	movl	-976(%rbp), %edi
	testl	%edi, %edi
	js	.L990
	call	close@PLT
.L990:
	movq	-1304(%rbp), %rax
	decq	%rax
	cmpq	$-3, %rax
	jbe	.L1459
.L991:
	movl	-1008(%rbp), %edi
	testl	%edi, %edi
	js	.L992
	call	close@PLT
.L992:
	movq	-1328(%rbp), %rax
	decq	%rax
	cmpq	$-3, %rax
	jbe	.L1460
.L993:
	movl	-1040(%rbp), %edi
	testl	%edi, %edi
	js	.L994
	call	close@PLT
.L994:
	movq	-720(%rbp), %rdi
	cmpq	-1320(%rbp), %rdi
	je	.L995
	movq	-704(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L995:
	movq	-1080(%rbp), %rbx
	movq	-1088(%rbp), %r12
	cmpq	%r12, %rbx
	je	.L996
.L1000:
	movq	(%r12), %rdi
	testq	%rdi, %rdi
	je	.L997
	movq	16(%r12), %rsi
	addq	$24, %r12
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
	cmpq	%r12, %rbx
	jne	.L1000
.L999:
	movq	-1088(%rbp), %r12
.L996:
	testq	%r12, %r12
	je	.L1001
	movq	-1072(%rbp), %rsi
	movq	%r12, %rdi
	subq	%r12, %rsi
	call	_ZdlPvm@PLT
.L1001:
	movq	-896(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L1002
	movq	-880(%rbp), %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L1002:
	movq	-1112(%rbp), %rbx
	movq	-1120(%rbp), %r12
	cmpq	%r12, %rbx
	je	.L1003
.L1007:
	movq	(%r12), %rdi
	leaq	16(%r12), %rax
	cmpq	%rax, %rdi
	je	.L1004
	movq	16(%r12), %rax
	addq	$32, %r12
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	cmpq	%rbx, %r12
	jne	.L1007
.L1006:
	movq	-1120(%rbp), %r12
.L1003:
	testq	%r12, %r12
	je	.L741
	movq	-1104(%rbp), %rsi
	movq	%r12, %rdi
	subq	%r12, %rsi
	call	_ZdlPvm@PLT
	jmp	.L741
.L1004:
	addq	$32, %r12
	cmpq	%r12, %rbx
	jne	.L1007
	jmp	.L1006
.L997:
	addq	$24, %r12
	cmpq	%r12, %rbx
	jne	.L1000
	jmp	.L999
.L783:
	movq	%r12, %rdi
.LEHB61:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L784
	movq	%r12, %rdi
	call	*%rax
.LEHE61:
	movsbl	%al, %esi
	jmp	.L784
.L754:
	movq	%r12, %rdi
.LEHB62:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L755
	movq	%r12, %rdi
	call	*%rax
.LEHE62:
	movsbl	%al, %esi
	jmp	.L755
.L1400:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$18, %edx
	leaq	.LC7(%rip), %rsi
	movq	%r12, %rdi
.LEHB63:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %r12
	testq	%r12, %r12
	je	.L1461
	cmpb	$0, 56(%r12)
	je	.L728
	movsbl	67(%r12), %esi
.L729:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	%r13d, %edi
	call	close@PLT
.LEHE63:
	movl	$-1, -1136(%rbp)
	jmp	.L730
.L1394:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$18, %edx
	leaq	.LC7(%rip), %rsi
	movq	%r12, %rdi
.LEHB64:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %r12
	testq	%r12, %r12
	je	.L1462
	cmpb	$0, 56(%r12)
	je	.L703
	movsbl	67(%r12), %esi
.L704:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	%r13d, %edi
	call	close@PLT
.LEHE64:
	movl	$-1, -1168(%rbp)
	jmp	.L705
.L1388:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$18, %edx
	leaq	.LC7(%rip), %rsi
	movq	%r12, %rdi
.LEHB65:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %r12
	testq	%r12, %r12
	je	.L1463
	cmpb	$0, 56(%r12)
	je	.L678
	movsbl	67(%r12), %esi
.L679:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	%r13d, %edi
	call	close@PLT
.LEHE65:
	movl	$-1, -1200(%rbp)
	jmp	.L680
.L820:
	testq	%r12, %r12
	jne	.L1464
	movq	-1240(%rbp), %rax
	jmp	.L821
.L798:
	testq	%r12, %r12
	jne	.L1465
	movq	-1240(%rbp), %rax
	jmp	.L799
.L788:
	testq	%r12, %r12
	jne	.L1466
	movq	-1320(%rbp), %rax
	jmp	.L789
.L809:
	testq	%r12, %r12
	jne	.L1467
	movq	-1240(%rbp), %rax
	jmp	.L810
.L1403:
	leaq	-800(%rbp), %rsi
	leaq	-592(%rbp), %rdi
	xorl	%edx, %edx
.LEHB66:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE66:
	movq	%rax, -592(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -576(%rbp)
.L744:
	movq	%r12, %rdx
	movq	%r13, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-592(%rbp), %rax
	jmp	.L746
.L1435:
	movq	-1256(%rbp), %rsi
	xorl	%edx, %edx
	movq	%r15, %rdi
.LEHB67:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE67:
	movq	%rax, -592(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -576(%rbp)
.L819:
	movq	%r12, %rdx
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-592(%rbp), %rax
	jmp	.L821
.L1429:
	movq	-1256(%rbp), %rsi
	xorl	%edx, %edx
	movq	%r15, %rdi
.LEHB68:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE68:
	movq	%rax, -592(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -576(%rbp)
.L797:
	movq	%r12, %rdx
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-592(%rbp), %rax
	jmp	.L799
.L1426:
	movq	-1256(%rbp), %rsi
	leaq	-720(%rbp), %rdi
	xorl	%edx, %edx
.LEHB69:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE69:
	movq	%rax, -720(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -704(%rbp)
.L787:
	movq	%r12, %rdx
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-720(%rbp), %rax
	jmp	.L789
.L1432:
	movq	-1256(%rbp), %rsi
	xorl	%edx, %edx
	movq	%r15, %rdi
.LEHB70:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE70:
	movq	%rax, -592(%rbp)
	movq	%rax, %rdi
	movq	-800(%rbp), %rax
	movq	%rax, -576(%rbp)
.L808:
	movq	%r12, %rdx
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	-800(%rbp), %r12
	movq	-592(%rbp), %rax
	jmp	.L810
.L831:
	movq	%r13, %rdi
.LEHB71:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	0(%r13), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L832
	movq	%r13, %rdi
	call	*%rax
.LEHE71:
	movsbl	%al, %esi
	jmp	.L832
.L861:
	movq	$0, -928(%rbp)
	movq	$0, -912(%rbp)
	movq	$0, -920(%rbp)
.L865:
	movq	-1336(%rbp), %rax
	vmovdqa	-1360(%rbp), %xmm6
	movq	%rax, -776(%rbp)
	movq	-1264(%rbp), %rax
	vmovdqa	%xmm6, -800(%rbp)
	vmovq	-1384(%rbp), %xmm6
	movq	%rax, -784(%rbp)
	movq	-1256(%rbp), %rsi
	leaq	-928(%rbp), %rax
	vpinsrq	$1, %rax, %xmm6, %xmm0
	xorl	%ecx, %ecx
	xorl	%edx, %edx
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3(%rip), %rdi
	vmovdqa	%xmm0, -768(%rbp)
	call	GOMP_parallel@PLT
	vpxor	%xmm0, %xmm0, %xmm0
	movq	$0, -624(%rbp)
	vmovdqa	%ymm0, -656(%rbp)
	jmp	.L1048
.L859:
	movq	%r12, %rdi
.LEHB72:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L860
	movq	%r12, %rdi
	call	*%rax
.LEHE72:
	movsbl	%al, %esi
	jmp	.L860
.L879:
	testq	%r12, %r12
	jne	.L1468
	movq	-1240(%rbp), %rax
	jmp	.L880
.L1455:
	movq	-1424(%rbp), %rsi
	xorl	%edx, %edx
	movq	%r15, %rdi
.LEHB73:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE73:
	movq	%rax, -592(%rbp)
	movq	%rax, %rdi
	movq	-1224(%rbp), %rax
	movq	%rax, -576(%rbp)
.L878:
	movq	-1360(%rbp), %rsi
	movq	%r12, %rdx
	call	memcpy@PLT
	movq	-1224(%rbp), %r12
	movq	-592(%rbp), %rax
	jmp	.L880
.L1445:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$28, %edx
	leaq	.LC48(%rip), %rsi
	movq	%r12, %rdi
.LEHB74:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-680(%rbp), %rdx
	movq	-688(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
.LEHE74:
	movq	%rax, %r14
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r14,%rax), %r12
	testq	%r12, %r12
	je	.L1469
	cmpb	$0, 56(%r12)
	je	.L914
	movsbl	67(%r12), %esi
	jmp	.L915
.L1456:
	vmovdqa	-576(%rbp), %xmm7
	vmovdqu	%xmm7, 16(%r13)
	jmp	.L883
.L898:
	testq	%r12, %r12
	jne	.L1470
	movq	-1248(%rbp), %rax
	jmp	.L899
.L1460:
	movq	-1048(%rbp), %rsi
	movq	-1328(%rbp), %rdi
	call	munmap@PLT
	jmp	.L993
.L1422:
	vzeroupper
	jmp	.L771
.L1459:
	movq	-1016(%rbp), %rsi
	movq	-1304(%rbp), %rdi
	call	munmap@PLT
	jmp	.L991
.L1458:
	movq	-984(%rbp), %rsi
	movq	-1312(%rbp), %rdi
	call	munmap@PLT
	jmp	.L989
.L678:
	movq	%r12, %rdi
.LEHB75:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L679
	movq	%r12, %rdi
	call	*%rax
.LEHE75:
	movsbl	%al, %esi
	jmp	.L679
.L1401:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$17, %edx
	leaq	.LC8(%rip), %rsi
	movq	%r12, %rdi
.LEHB76:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
.LEHE76:
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %r12
	testq	%r12, %r12
	je	.L1471
	cmpb	$0, 56(%r12)
	je	.L732
	movsbl	67(%r12), %esi
	jmp	.L733
.L1395:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$17, %edx
	leaq	.LC8(%rip), %rsi
	movq	%r12, %rdi
.LEHB77:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
.LEHE77:
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %r12
	testq	%r12, %r12
	je	.L1472
	cmpb	$0, 56(%r12)
	je	.L707
	movsbl	67(%r12), %esi
	jmp	.L708
.L728:
	movq	%r12, %rdi
.LEHB78:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L729
	movq	%r12, %rdi
	call	*%rax
.LEHE78:
	movsbl	%al, %esi
	jmp	.L729
.L1389:
	leaq	_ZSt4cerr(%rip), %r12
	movl	$17, %edx
	leaq	.LC8(%rip), %rsi
	movq	%r12, %rdi
.LEHB79:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	-648(%rbp), %rdx
	movq	-656(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
.LEHE79:
	movq	%rax, %r15
	movq	(%rax), %rax
	movq	-24(%rax), %rax
	movq	240(%r15,%rax), %r12
	testq	%r12, %r12
	je	.L1473
	cmpb	$0, 56(%r12)
	je	.L682
	movsbl	67(%r12), %esi
	jmp	.L683
.L703:
	movq	%r12, %rdi
.LEHB80:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L704
	movq	%r12, %rdi
	call	*%rax
.LEHE80:
	movsbl	%al, %esi
	jmp	.L704
.L881:
	movq	-1256(%rbp), %rdi
	movq	%r15, %rdx
	movq	%r13, %rsi
.LEHB81:
	call	_ZNSt6vectorISt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESaIS7_EE17_M_realloc_insertIJS7_EEEvN9__gnu_cxx17__normal_iteratorIPS7_S9_EEDpOT_
.LEHE81:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1378
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L1378:
	movq	-784(%rbp), %rax
	movq	-792(%rbp), %r13
	movq	%rax, -1280(%rbp)
	jmp	.L888
.L1442:
	leaq	-1224(%rbp), %rsi
	leaq	-688(%rbp), %rdi
	xorl	%edx, %edx
.LEHB82:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm@PLT
.LEHE82:
	movq	%rax, -688(%rbp)
	movq	%rax, %rdi
	movq	-1224(%rbp), %rax
	movq	%rax, -672(%rbp)
.L897:
	movq	%r12, %rdx
	movq	%rbx, %rsi
	call	memcpy@PLT
	movq	-1224(%rbp), %r12
	movq	-688(%rbp), %rax
	jmp	.L899
.L941:
	movq	%r12, %rdi
.LEHB83:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L942
	movq	%r12, %rdi
	call	*%rax
.LEHE83:
	movsbl	%al, %esi
	jmp	.L942
.L890:
	movq	-1240(%rbp), %rdi
	movq	%r13, %rsi
	call	_ZSt16__insertion_sortIN9__gnu_cxx17__normal_iteratorIPSt4pairINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEElESt6vectorIS9_SaIS9_EEEENS0_5__ops15_Iter_comp_iterIZ6run_q4RKS8_SI_EUlRKT_RKT0_E_EEEvSJ_SJ_SM_.constprop.0
	jmp	.L889
.L1444:
	movl	32(%rdi), %esi
	orl	$4, %esi
.LEHB84:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate@PLT
.LEHE84:
	jmp	.L906
.L682:
	movq	%r12, %rdi
.LEHB85:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	jne	.L1474
.L683:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	%r13d, %edi
	call	close@PLT
.LEHE85:
	movl	$-1, -1200(%rbp)
	movq	$0, -1216(%rbp)
	movq	$0, -1208(%rbp)
	jmp	.L680
.L707:
	movq	%r12, %rdi
.LEHB86:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	jne	.L1475
.L708:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	%r13d, %edi
	call	close@PLT
.LEHE86:
	movl	$-1, -1168(%rbp)
	movq	$0, -1184(%rbp)
	movq	$0, -1176(%rbp)
	jmp	.L705
.L732:
	movq	%r12, %rdi
.LEHB87:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	jne	.L1476
.L733:
	movq	%r15, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	%r13d, %edi
	call	close@PLT
.LEHE87:
	movl	$-1, -1136(%rbp)
	movq	$0, -1152(%rbp)
	movq	$0, -1144(%rbp)
	jmp	.L730
.L914:
	movq	%r12, %rdi
.LEHB88:
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	jne	.L1477
.L915:
	movq	%r14, %rdi
	call	_ZNSo3putEc@PLT
	vmovq	.LC56(%rip), %xmm6
	movq	%rax, %rdi
	leaq	16+_ZTVSt13basic_filebufIcSt11char_traitsIcEE(%rip), %rax
	vpinsrq	$1, %rax, %xmm6, %xmm5
	vmovdqa	%xmm5, -1376(%rbp)
	call	_ZNSo5flushEv@PLT
.LEHE88:
	vmovdqa	-1376(%rbp), %xmm5
	movq	-1256(%rbp), %rdi
	leaq	64+_ZTVSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -344(%rbp)
	vmovdqa	%xmm5, -592(%rbp)
.LEHB89:
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEE5closeEv@PLT
.LEHE89:
.L919:
	movq	-1360(%rbp), %rdi
	call	_ZNSt12__basic_fileIcED1Ev@PLT
	leaq	16+_ZTVSt15basic_streambufIcSt11char_traitsIcEE(%rip), %rax
	leaq	-528(%rbp), %rdi
	movq	%rax, -584(%rbp)
	call	_ZNSt6localeD1Ev@PLT
	movq	8+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	16+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rcx
	movq	%rax, -592(%rbp)
	movq	-24(%rax), %rax
	movq	-1336(%rbp), %rdi
	movq	%rcx, -592(%rbp,%rax)
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -344(%rbp)
	call	_ZNSt8ios_baseD2Ev@PLT
	movq	-688(%rbp), %rdi
	cmpq	-1248(%rbp), %rdi
	je	.L917
	movq	-672(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L917:
	movq	-1240(%rbp), %rax
	movq	%rax, %rbx
	cmpq	%r13, %rax
	je	.L925
.L920:
	movq	(%rbx), %rdi
	leaq	16(%rbx), %rax
	cmpq	%rax, %rdi
	je	.L923
	movq	16(%rbx), %rax
	addq	$40, %rbx
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	cmpq	%r13, %rbx
	jne	.L920
.L925:
	cmpq	$0, -1240(%rbp)
	je	.L922
	movq	-1280(%rbp), %rsi
	movq	-1240(%rbp), %rdi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L922:
	movq	-928(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L926
	movq	-912(%rbp), %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L926:
	movq	-848(%rbp), %rdi
	testq	%rdi, %rdi
	je	.L927
	movq	-832(%rbp), %rsi
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
.L927:
	movq	-952(%rbp), %rbx
	movq	-960(%rbp), %r12
	cmpq	%r12, %rbx
	je	.L928
.L932:
	movq	(%r12), %rdi
	testq	%rdi, %rdi
	je	.L929
	movq	16(%r12), %rsi
	addq	$24, %r12
	subq	%rdi, %rsi
	call	_ZdlPvm@PLT
	cmpq	%r12, %rbx
	jne	.L932
.L931:
	movq	-960(%rbp), %r12
.L928:
	testq	%r12, %r12
	je	.L934
	movq	-944(%rbp), %rsi
	movq	%r12, %rdi
	subq	%r12, %rsi
	call	_ZdlPvm@PLT
	jmp	.L934
.L929:
	addq	$24, %r12
	cmpq	%r12, %rbx
	jne	.L932
	jmp	.L931
.L923:
	addq	$40, %rbx
	cmpq	%r13, %rbx
	jne	.L920
	jmp	.L925
.L1453:
	movq	-1408(%rbp), %rsi
	movq	-1296(%rbp), %rdi
	call	munmap@PLT
	jmp	.L985
.L1452:
	movq	-1176(%rbp), %rsi
	movq	-1288(%rbp), %rdi
	call	munmap@PLT
	jmp	.L983
.L1451:
	movq	-1144(%rbp), %rsi
	movq	-1264(%rbp), %rdi
	call	munmap@PLT
	jmp	.L981
.L1450:
	movq	-1416(%rbp), %rsi
	movq	-1328(%rbp), %rdi
	call	munmap@PLT
	jmp	.L965
.L1449:
	movq	-1016(%rbp), %rsi
	movq	-1304(%rbp), %rdi
	call	munmap@PLT
	jmp	.L963
.L1448:
	movq	-984(%rbp), %rsi
	movq	-1312(%rbp), %rdi
	call	munmap@PLT
	jmp	.L961
.L869:
	movq	-1336(%rbp), %rax
	vmovdqa	-1360(%rbp), %xmm4
	movq	%rax, -776(%rbp)
	movq	-1256(%rbp), %rsi
	movq	-1264(%rbp), %rax
	xorl	%ecx, %ecx
	xorl	%edx, %edx
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.3(%rip), %rdi
	movq	%rax, -784(%rbp)
	vmovdqa	%xmm4, -800(%rbp)
	vmovdqa	%xmm1, -768(%rbp)
	vzeroupper
	call	GOMP_parallel@PLT
	vpxor	%xmm0, %xmm0, %xmm0
	cmpl	$3, -1280(%rbp)
	movq	$0, -624(%rbp)
	movq	-928(%rbp), %rdx
	vmovdqa	%ymm0, -656(%rbp)
	jg	.L1049
	xorl	%r10d, %r10d
	xorl	%eax, %eax
	xorl	%esi, %esi
	xorl	%ecx, %ecx
	xorl	%edi, %edi
	xorl	%r8d, %r8d
	xorl	%r9d, %r9d
	jmp	.L1047
.L1437:
	vzeroupper
	jmp	.L837
.L1077:
	movq	%r13, %rdx
	jmp	.L835
.L1446:
	movq	-592(%rbp), %rax
	movq	-24(%rax), %rdi
	addq	%r15, %rdi
	movl	32(%rdi), %esi
	orl	$4, %esi
.LEHB90:
	call	_ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate@PLT
.LEHE90:
	jmp	.L939
.L1078:
	xorl	%eax, %eax
	xorl	%edi, %edi
	xorl	%edx, %edx
	jmp	.L839
.L834:
	movq	-1312(%rbp), %rax
	movq	-1256(%rbp), %rsi
	movq	%rax, -776(%rbp)
	movq	-1304(%rbp), %rax
	xorl	%ecx, %ecx
	movq	%rax, -784(%rbp)
	movq	-1328(%rbp), %rax
	xorl	%edx, %edx
	movq	%rax, -792(%rbp)
	movq	-1400(%rbp), %rax
	leaq	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_._omp_fn.2(%rip), %rdi
	movq	%rax, -800(%rbp)
	leaq	-960(%rbp), %rax
	movq	$0, -960(%rbp)
	movq	$0, -944(%rbp)
	movq	$0, -952(%rbp)
	movq	%rbx, -768(%rbp)
	movq	%rax, -1400(%rbp)
	movq	%rax, -760(%rbp)
	call	GOMP_parallel@PLT
.L838:
	movl	$1, %ebx
	jmp	.L843
.L862:
	vmovq	-1384(%rbp), %xmm7
	leaq	-928(%rbp), %rax
	vpinsrq	$1, %rax, %xmm7, %xmm1
	movq	%rcx, -920(%rbp)
	movl	$1, %ebx
	xorl	%eax, %eax
	jmp	.L866
.L1070:
	movq	%r13, %rax
	jmp	.L769
.L1421:
	leaq	.LC27(%rip), %rdi
.LEHB91:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE91:
.L1439:
.LEHB92:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE92:
.L1081:
	xorl	%eax, %eax
	jmp	.L866
.L1366:
	vzeroupper
	jmp	.L865
.L1443:
	leaq	.LC14(%rip), %rdi
.LEHB93:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE93:
.L1441:
	leaq	.LC9(%rip), %rdi
.LEHB94:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE94:
.L1447:
.LEHB95:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE95:
.L1457:
.LEHB96:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE96:
.L1454:
	leaq	.LC9(%rip), %rdi
.LEHB97:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE97:
.L1469:
.LEHB98:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE98:
.L1471:
.LEHB99:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE99:
.L1468:
	movq	-1240(%rbp), %rdi
	jmp	.L878
.L1467:
	movq	-1240(%rbp), %rdi
	jmp	.L808
.L1473:
.LEHB100:
	call	_ZSt16__throw_bad_castv@PLT
	.p2align 4,,10
	.p2align 3
.L1474:
	movq	%r12, %rdi
	call	*%rax
.LEHE100:
	movsbl	%al, %esi
	jmp	.L683
.L1472:
.LEHB101:
	call	_ZSt16__throw_bad_castv@PLT
	.p2align 4,,10
	.p2align 3
.L1475:
	movq	%r12, %rdi
	call	*%rax
.LEHE101:
	movsbl	%al, %esi
	jmp	.L708
.L1466:
	movq	-1320(%rbp), %rdi
	jmp	.L787
.L1465:
	movq	-1240(%rbp), %rdi
	jmp	.L797
.L1464:
	movq	-1240(%rbp), %rdi
	jmp	.L819
.L1463:
.LEHB102:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE102:
	.p2align 4,,10
	.p2align 3
.L1476:
	movq	%r12, %rdi
.LEHB103:
	call	*%rax
.LEHE103:
	movsbl	%al, %esi
	jmp	.L733
.L1383:
	leaq	.LC14(%rip), %rdi
.LEHB104:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE104:
.L1384:
	leaq	.LC9(%rip), %rdi
.LEHB105:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE105:
.L1386:
	leaq	.LC14(%rip), %rdi
.LEHB106:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE106:
.L1381:
	leaq	.LC9(%rip), %rdi
.LEHB107:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE107:
.L1413:
.LEHB108:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE108:
.L1414:
.LEHB109:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE109:
.L1415:
.LEHB110:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE110:
.L1402:
	leaq	.LC9(%rip), %rdi
.LEHB111:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE111:
.L1390:
	leaq	.LC9(%rip), %rdi
.LEHB112:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE112:
.L1392:
	leaq	.LC14(%rip), %rdi
.LEHB113:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE113:
.L1396:
	leaq	.LC9(%rip), %rdi
.LEHB114:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE114:
.L1398:
	leaq	.LC14(%rip), %rdi
.LEHB115:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE115:
.L1404:
	movq	-1272(%rbp), %rdi
	jmp	.L654
.L1405:
.LEHB116:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE116:
.L1409:
	call	__stack_chk_fail@PLT
.L1410:
	movq	%rbx, %rdi
	jmp	.L688
.L1411:
	movq	%rbx, %rdi
	jmp	.L663
.L1412:
	movq	%rbx, %rdi
	jmp	.L713
.L1462:
.LEHB117:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE117:
.L1461:
.LEHB118:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE118:
	.p2align 4,,10
	.p2align 3
.L1477:
	movq	%r12, %rdi
.LEHB119:
	call	*%rax
.LEHE119:
	movsbl	%al, %esi
	jmp	.L915
.L1470:
	movq	-1248(%rbp), %rdi
	jmp	.L897
	.p2align 4,,10
	.p2align 3
.L1419:
.LEHB120:
	call	_ZSt16__throw_bad_castv@PLT
.L1418:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE120:
.L1417:
	leaq	.LC14(%rip), %rdi
.LEHB121:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE121:
.L1416:
	movq	-1240(%rbp), %rdi
	jmp	.L744
.L1436:
	leaq	.LC14(%rip), %rdi
.LEHB122:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE122:
.L1434:
	leaq	.LC9(%rip), %rdi
.LEHB123:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE123:
.L1433:
	leaq	.LC14(%rip), %rdi
.LEHB124:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE124:
.L1431:
	leaq	.LC9(%rip), %rdi
.LEHB125:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE125:
.L1430:
	leaq	.LC14(%rip), %rdi
.LEHB126:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE126:
.L1428:
	leaq	.LC9(%rip), %rdi
.LEHB127:
	call	_ZSt19__throw_logic_errorPKc@PLT
.LEHE127:
.L1427:
	leaq	.LC14(%rip), %rdi
.LEHB128:
	call	_ZSt20__throw_length_errorPKc@PLT
.LEHE128:
.L1425:
	leaq	.LC9(%rip), %rdi
.LEHB129:
	call	_ZSt19__throw_logic_errorPKc@PLT
.L1424:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE129:
.L1118:
	endbr64
	movq	%rax, %rbx
	jmp	.L792
.L1099:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L794
.L1115:
	endbr64
	movq	%rax, %r12
	jmp	.L718
.L1121:
	endbr64
	movq	%rax, %rbx
	jmp	.L824
.L1101:
	endbr64
	movq	%rax, %rbx
	jmp	.L1024
.L1119:
	endbr64
	movq	%rax, %rbx
	jmp	.L802
.L1103:
	endbr64
	movq	%rax, %rbx
	jmp	.L1026
.L1120:
	endbr64
	movq	%rax, %rbx
	jmp	.L813
.L1128:
	endbr64
	movq	%rax, %rdi
	jmp	.L945
.L1109:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L904
.L1102:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L815
.L1100:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L804
.L1104:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L826
.L1098:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L1044
.L1093:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L720
.L1089:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L670
.L1094:
	endbr64
	movq	%rax, %r12
	jmp	.L1020
.L1095:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L751
.L1113:
	endbr64
	movq	%rax, %r12
	jmp	.L659
.L1114:
	endbr64
	movq	%rax, %r12
	jmp	.L693
.L1092:
	endbr64
	movq	%rax, %r12
	jmp	.L1018
.L1112:
	endbr64
	movq	%rax, %r12
	jmp	.L668
.L1090:
	endbr64
	movq	%rax, %r12
	jmp	.L1016
.L1091:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L695
.L1097:
	endbr64
	movq	%rax, %r12
	vzeroupper
	jmp	.L767
.L1096:
	endbr64
	movq	%rax, %r12
	jmp	.L1022
.L1110:
	endbr64
	movq	%rax, %rbx
	jmp	.L1030
.L1116:
	endbr64
	movq	%rax, %r12
	jmp	.L749
.L1107:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L1040
.L1122:
	endbr64
	movq	%rax, %rbx
	jmp	.L847
.L1106:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L1042
.L1105:
	endbr64
	movq	%rax, %rbx
	jmp	.L1028
.L1117:
	endbr64
	movq	%rax, %r12
	jmp	.L765
.L1111:
	endbr64
	movq	%rax, %rbx
	jmp	.L1032
.L1127:
	endbr64
	movq	%rax, %rdi
	jmp	.L918
.L1108:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L1038
.L1126:
	endbr64
	movq	%rax, %rbx
	jmp	.L909
.L1125:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L910
.L1124:
	endbr64
	movq	%rax, %rbx
	vzeroupper
	jmp	.L911
.L1123:
	endbr64
	movq	%rax, %rbx
	jmp	.L902
	.section	.gcc_except_table
	.align 4
.LLSDA3923:
	.byte	0xff
	.byte	0x9b
	.uleb128 .LLSDATT3923-.LLSDATTD3923
.LLSDATTD3923:
	.byte	0x1
	.uleb128 .LLSDACSE3923-.LLSDACSB3923
.LLSDACSB3923:
	.uleb128 .LEHB18-.LFB3923
	.uleb128 .LEHE18-.LEHB18
	.uleb128 .L1113-.LFB3923
	.uleb128 0
	.uleb128 .LEHB19-.LFB3923
	.uleb128 .LEHE19-.LEHB19
	.uleb128 .L1112-.LFB3923
	.uleb128 0
	.uleb128 .LEHB20-.LFB3923
	.uleb128 .LEHE20-.LEHB20
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB21-.LFB3923
	.uleb128 .LEHE21-.LEHB21
	.uleb128 .L1114-.LFB3923
	.uleb128 0
	.uleb128 .LEHB22-.LFB3923
	.uleb128 .LEHE22-.LEHB22
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB23-.LFB3923
	.uleb128 .LEHE23-.LEHB23
	.uleb128 .L1115-.LFB3923
	.uleb128 0
	.uleb128 .LEHB24-.LFB3923
	.uleb128 .LEHE24-.LEHB24
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB25-.LFB3923
	.uleb128 .LEHE25-.LEHB25
	.uleb128 .L1095-.LFB3923
	.uleb128 0
	.uleb128 .LEHB26-.LFB3923
	.uleb128 .LEHE26-.LEHB26
	.uleb128 .L1089-.LFB3923
	.uleb128 0
	.uleb128 .LEHB27-.LFB3923
	.uleb128 .LEHE27-.LEHB27
	.uleb128 0
	.uleb128 0
	.uleb128 .LEHB28-.LFB3923
	.uleb128 .LEHE28-.LEHB28
	.uleb128 .L1091-.LFB3923
	.uleb128 0
	.uleb128 .LEHB29-.LFB3923
	.uleb128 .LEHE29-.LEHB29
	.uleb128 .L1093-.LFB3923
	.uleb128 0
	.uleb128 .LEHB30-.LFB3923
	.uleb128 .LEHE30-.LEHB30
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB31-.LFB3923
	.uleb128 .LEHE31-.LEHB31
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB32-.LFB3923
	.uleb128 .LEHE32-.LEHB32
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB33-.LFB3923
	.uleb128 .LEHE33-.LEHB33
	.uleb128 .L1095-.LFB3923
	.uleb128 0
	.uleb128 .LEHB34-.LFB3923
	.uleb128 .LEHE34-.LEHB34
	.uleb128 .L1116-.LFB3923
	.uleb128 0
	.uleb128 .LEHB35-.LFB3923
	.uleb128 .LEHE35-.LEHB35
	.uleb128 .L1096-.LFB3923
	.uleb128 0
	.uleb128 .LEHB36-.LFB3923
	.uleb128 .LEHE36-.LEHB36
	.uleb128 .L1097-.LFB3923
	.uleb128 0
	.uleb128 .LEHB37-.LFB3923
	.uleb128 .LEHE37-.LEHB37
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB38-.LFB3923
	.uleb128 .LEHE38-.LEHB38
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB39-.LFB3923
	.uleb128 .LEHE39-.LEHB39
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB40-.LFB3923
	.uleb128 .LEHE40-.LEHB40
	.uleb128 .L1117-.LFB3923
	.uleb128 0
	.uleb128 .LEHB41-.LFB3923
	.uleb128 .LEHE41-.LEHB41
	.uleb128 .L1098-.LFB3923
	.uleb128 0
	.uleb128 .LEHB42-.LFB3923
	.uleb128 .LEHE42-.LEHB42
	.uleb128 .L1099-.LFB3923
	.uleb128 0
	.uleb128 .LEHB43-.LFB3923
	.uleb128 .LEHE43-.LEHB43
	.uleb128 .L1118-.LFB3923
	.uleb128 0
	.uleb128 .LEHB44-.LFB3923
	.uleb128 .LEHE44-.LEHB44
	.uleb128 .L1119-.LFB3923
	.uleb128 0
	.uleb128 .LEHB45-.LFB3923
	.uleb128 .LEHE45-.LEHB45
	.uleb128 .L1101-.LFB3923
	.uleb128 0
	.uleb128 .LEHB46-.LFB3923
	.uleb128 .LEHE46-.LEHB46
	.uleb128 .L1120-.LFB3923
	.uleb128 0
	.uleb128 .LEHB47-.LFB3923
	.uleb128 .LEHE47-.LEHB47
	.uleb128 .L1103-.LFB3923
	.uleb128 0
	.uleb128 .LEHB48-.LFB3923
	.uleb128 .LEHE48-.LEHB48
	.uleb128 .L1121-.LFB3923
	.uleb128 0
	.uleb128 .LEHB49-.LFB3923
	.uleb128 .LEHE49-.LEHB49
	.uleb128 .L1105-.LFB3923
	.uleb128 0
	.uleb128 .LEHB50-.LFB3923
	.uleb128 .LEHE50-.LEHB50
	.uleb128 .L1106-.LFB3923
	.uleb128 0
	.uleb128 .LEHB51-.LFB3923
	.uleb128 .LEHE51-.LEHB51
	.uleb128 .L1122-.LFB3923
	.uleb128 0
	.uleb128 .LEHB52-.LFB3923
	.uleb128 .LEHE52-.LEHB52
	.uleb128 .L1107-.LFB3923
	.uleb128 0
	.uleb128 .LEHB53-.LFB3923
	.uleb128 .LEHE53-.LEHB53
	.uleb128 .L1108-.LFB3923
	.uleb128 0
	.uleb128 .LEHB54-.LFB3923
	.uleb128 .LEHE54-.LEHB54
	.uleb128 .L1123-.LFB3923
	.uleb128 0
	.uleb128 .LEHB55-.LFB3923
	.uleb128 .LEHE55-.LEHB55
	.uleb128 .L1124-.LFB3923
	.uleb128 0
	.uleb128 .LEHB56-.LFB3923
	.uleb128 .LEHE56-.LEHB56
	.uleb128 .L1125-.LFB3923
	.uleb128 0
	.uleb128 .LEHB57-.LFB3923
	.uleb128 .LEHE57-.LEHB57
	.uleb128 .L1126-.LFB3923
	.uleb128 0
	.uleb128 .LEHB58-.LFB3923
	.uleb128 .LEHE58-.LEHB58
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB59-.LFB3923
	.uleb128 .LEHE59-.LEHB59
	.uleb128 .L1128-.LFB3923
	.uleb128 0x1
	.uleb128 .LEHB60-.LFB3923
	.uleb128 .LEHE60-.LEHB60
	.uleb128 .L1106-.LFB3923
	.uleb128 0
	.uleb128 .LEHB61-.LFB3923
	.uleb128 .LEHE61-.LEHB61
	.uleb128 .L1099-.LFB3923
	.uleb128 0
	.uleb128 .LEHB62-.LFB3923
	.uleb128 .LEHE62-.LEHB62
	.uleb128 .L1097-.LFB3923
	.uleb128 0
	.uleb128 .LEHB63-.LFB3923
	.uleb128 .LEHE63-.LEHB63
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB64-.LFB3923
	.uleb128 .LEHE64-.LEHB64
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB65-.LFB3923
	.uleb128 .LEHE65-.LEHB65
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB66-.LFB3923
	.uleb128 .LEHE66-.LEHB66
	.uleb128 .L1095-.LFB3923
	.uleb128 0
	.uleb128 .LEHB67-.LFB3923
	.uleb128 .LEHE67-.LEHB67
	.uleb128 .L1104-.LFB3923
	.uleb128 0
	.uleb128 .LEHB68-.LFB3923
	.uleb128 .LEHE68-.LEHB68
	.uleb128 .L1100-.LFB3923
	.uleb128 0
	.uleb128 .LEHB69-.LFB3923
	.uleb128 .LEHE69-.LEHB69
	.uleb128 .L1099-.LFB3923
	.uleb128 0
	.uleb128 .LEHB70-.LFB3923
	.uleb128 .LEHE70-.LEHB70
	.uleb128 .L1102-.LFB3923
	.uleb128 0
	.uleb128 .LEHB71-.LFB3923
	.uleb128 .LEHE71-.LEHB71
	.uleb128 .L1106-.LFB3923
	.uleb128 0
	.uleb128 .LEHB72-.LFB3923
	.uleb128 .LEHE72-.LEHB72
	.uleb128 .L1107-.LFB3923
	.uleb128 0
	.uleb128 .LEHB73-.LFB3923
	.uleb128 .LEHE73-.LEHB73
	.uleb128 .L1109-.LFB3923
	.uleb128 0
	.uleb128 .LEHB74-.LFB3923
	.uleb128 .LEHE74-.LEHB74
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB75-.LFB3923
	.uleb128 .LEHE75-.LEHB75
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB76-.LFB3923
	.uleb128 .LEHE76-.LEHB76
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB77-.LFB3923
	.uleb128 .LEHE77-.LEHB77
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB78-.LFB3923
	.uleb128 .LEHE78-.LEHB78
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB79-.LFB3923
	.uleb128 .LEHE79-.LEHB79
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB80-.LFB3923
	.uleb128 .LEHE80-.LEHB80
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB81-.LFB3923
	.uleb128 .LEHE81-.LEHB81
	.uleb128 .L1110-.LFB3923
	.uleb128 0
	.uleb128 .LEHB82-.LFB3923
	.uleb128 .LEHE82-.LEHB82
	.uleb128 .L1109-.LFB3923
	.uleb128 0
	.uleb128 .LEHB83-.LFB3923
	.uleb128 .LEHE83-.LEHB83
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB84-.LFB3923
	.uleb128 .LEHE84-.LEHB84
	.uleb128 .L1126-.LFB3923
	.uleb128 0
	.uleb128 .LEHB85-.LFB3923
	.uleb128 .LEHE85-.LEHB85
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB86-.LFB3923
	.uleb128 .LEHE86-.LEHB86
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB87-.LFB3923
	.uleb128 .LEHE87-.LEHB87
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB88-.LFB3923
	.uleb128 .LEHE88-.LEHB88
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB89-.LFB3923
	.uleb128 .LEHE89-.LEHB89
	.uleb128 .L1127-.LFB3923
	.uleb128 0x1
	.uleb128 .LEHB90-.LFB3923
	.uleb128 .LEHE90-.LEHB90
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB91-.LFB3923
	.uleb128 .LEHE91-.LEHB91
	.uleb128 .L1098-.LFB3923
	.uleb128 0
	.uleb128 .LEHB92-.LFB3923
	.uleb128 .LEHE92-.LEHB92
	.uleb128 .L1107-.LFB3923
	.uleb128 0
	.uleb128 .LEHB93-.LFB3923
	.uleb128 .LEHE93-.LEHB93
	.uleb128 .L1123-.LFB3923
	.uleb128 0
	.uleb128 .LEHB94-.LFB3923
	.uleb128 .LEHE94-.LEHB94
	.uleb128 .L1109-.LFB3923
	.uleb128 0
	.uleb128 .LEHB95-.LFB3923
	.uleb128 .LEHE95-.LEHB95
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB96-.LFB3923
	.uleb128 .LEHE96-.LEHB96
	.uleb128 .L1106-.LFB3923
	.uleb128 0
	.uleb128 .LEHB97-.LFB3923
	.uleb128 .LEHE97-.LEHB97
	.uleb128 .L1109-.LFB3923
	.uleb128 0
	.uleb128 .LEHB98-.LFB3923
	.uleb128 .LEHE98-.LEHB98
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB99-.LFB3923
	.uleb128 .LEHE99-.LEHB99
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB100-.LFB3923
	.uleb128 .LEHE100-.LEHB100
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB101-.LFB3923
	.uleb128 .LEHE101-.LEHB101
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB102-.LFB3923
	.uleb128 .LEHE102-.LEHB102
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB103-.LFB3923
	.uleb128 .LEHE103-.LEHB103
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB104-.LFB3923
	.uleb128 .LEHE104-.LEHB104
	.uleb128 .L1113-.LFB3923
	.uleb128 0
	.uleb128 .LEHB105-.LFB3923
	.uleb128 .LEHE105-.LEHB105
	.uleb128 .L1089-.LFB3923
	.uleb128 0
	.uleb128 .LEHB106-.LFB3923
	.uleb128 .LEHE106-.LEHB106
	.uleb128 .L1112-.LFB3923
	.uleb128 0
	.uleb128 .LEHB107-.LFB3923
	.uleb128 .LEHE107-.LEHB107
	.uleb128 0
	.uleb128 0
	.uleb128 .LEHB108-.LFB3923
	.uleb128 .LEHE108-.LEHB108
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB109-.LFB3923
	.uleb128 .LEHE109-.LEHB109
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB110-.LFB3923
	.uleb128 .LEHE110-.LEHB110
	.uleb128 .L1090-.LFB3923
	.uleb128 0
	.uleb128 .LEHB111-.LFB3923
	.uleb128 .LEHE111-.LEHB111
	.uleb128 .L1095-.LFB3923
	.uleb128 0
	.uleb128 .LEHB112-.LFB3923
	.uleb128 .LEHE112-.LEHB112
	.uleb128 .L1091-.LFB3923
	.uleb128 0
	.uleb128 .LEHB113-.LFB3923
	.uleb128 .LEHE113-.LEHB113
	.uleb128 .L1114-.LFB3923
	.uleb128 0
	.uleb128 .LEHB114-.LFB3923
	.uleb128 .LEHE114-.LEHB114
	.uleb128 .L1093-.LFB3923
	.uleb128 0
	.uleb128 .LEHB115-.LFB3923
	.uleb128 .LEHE115-.LEHB115
	.uleb128 .L1115-.LFB3923
	.uleb128 0
	.uleb128 .LEHB116-.LFB3923
	.uleb128 .LEHE116-.LEHB116
	.uleb128 .L1095-.LFB3923
	.uleb128 0
	.uleb128 .LEHB117-.LFB3923
	.uleb128 .LEHE117-.LEHB117
	.uleb128 .L1092-.LFB3923
	.uleb128 0
	.uleb128 .LEHB118-.LFB3923
	.uleb128 .LEHE118-.LEHB118
	.uleb128 .L1094-.LFB3923
	.uleb128 0
	.uleb128 .LEHB119-.LFB3923
	.uleb128 .LEHE119-.LEHB119
	.uleb128 .L1111-.LFB3923
	.uleb128 0
	.uleb128 .LEHB120-.LFB3923
	.uleb128 .LEHE120-.LEHB120
	.uleb128 .L1097-.LFB3923
	.uleb128 0
	.uleb128 .LEHB121-.LFB3923
	.uleb128 .LEHE121-.LEHB121
	.uleb128 .L1116-.LFB3923
	.uleb128 0
	.uleb128 .LEHB122-.LFB3923
	.uleb128 .LEHE122-.LEHB122
	.uleb128 .L1121-.LFB3923
	.uleb128 0
	.uleb128 .LEHB123-.LFB3923
	.uleb128 .LEHE123-.LEHB123
	.uleb128 .L1104-.LFB3923
	.uleb128 0
	.uleb128 .LEHB124-.LFB3923
	.uleb128 .LEHE124-.LEHB124
	.uleb128 .L1120-.LFB3923
	.uleb128 0
	.uleb128 .LEHB125-.LFB3923
	.uleb128 .LEHE125-.LEHB125
	.uleb128 .L1102-.LFB3923
	.uleb128 0
	.uleb128 .LEHB126-.LFB3923
	.uleb128 .LEHE126-.LEHB126
	.uleb128 .L1119-.LFB3923
	.uleb128 0
	.uleb128 .LEHB127-.LFB3923
	.uleb128 .LEHE127-.LEHB127
	.uleb128 .L1100-.LFB3923
	.uleb128 0
	.uleb128 .LEHB128-.LFB3923
	.uleb128 .LEHE128-.LEHB128
	.uleb128 .L1118-.LFB3923
	.uleb128 0
	.uleb128 .LEHB129-.LFB3923
	.uleb128 .LEHE129-.LEHB129
	.uleb128 .L1099-.LFB3923
	.uleb128 0
.LLSDACSE3923:
	.byte	0x1
	.byte	0
	.align 4
	.long	0

.LLSDATT3923:
	.text
	.cfi_endproc
	.section	.text.unlikely
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDAC3923
	.type	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_.cold, @function
_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_.cold:
.LFSB3923:
.L792:
	.cfi_escape 0xf,0x3,0x76,0x58,0x6
	.cfi_escape 0x10,0x3,0x2,0x76,0x50
	.cfi_escape 0x10,0x6,0x2,0x76,0
	.cfi_escape 0x10,0xc,0x2,0x76,0x60
	.cfi_escape 0x10,0xd,0x2,0x76,0x68
	.cfi_escape 0x10,0xe,0x2,0x76,0x70
	.cfi_escape 0x10,0xf,0x2,0x76,0x78
	movq	-720(%rbp), %rdi
	cmpq	-1320(%rbp), %rdi
	je	.L1360
	movq	-704(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L794:
	movq	-1392(%rbp), %rdi
	call	_ZNSt6vectorIS_IiSaIiEESaIS1_EED1Ev
.L1044:
	movq	-896(%rbp), %rdi
	movq	-880(%rbp), %rsi
	subq	%rdi, %rsi
	testq	%rdi, %rdi
	je	.L1045
	call	_ZdlPvm@PLT
.L1045:
	movq	%rbx, %r12
.L767:
	movq	-1432(%rbp), %rdi
	call	_ZNSt6vectorINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESaIS5_EED1Ev
.L751:
	leaq	-1152(%rbp), %rdi
	call	_ZN8MmapFileD1Ev
	jmp	.L720
.L1360:
	vzeroupper
	jmp	.L794
.L718:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L1356
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L720:
	leaq	-1184(%rbp), %rdi
	call	_ZN8MmapFileD1Ev
.L695:
	leaq	-1216(%rbp), %rdi
	call	_ZN8MmapFileD1Ev
.L670:
	movq	-752(%rbp), %rdi
	cmpq	-1272(%rbp), %rdi
	je	.L1046
	movq	-736(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L1046:
	movq	%r12, %rdi
.LEHB130:
	call	_Unwind_Resume@PLT
.LEHE130:
.L824:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1363
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L826:
	movq	-1448(%rbp), %rdi
	call	_ZN8MmapFileD1Ev
	jmp	.L815
.L1356:
	vzeroupper
	jmp	.L720
.L1024:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1372
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L804:
	movq	-720(%rbp), %rdi
	cmpq	-1320(%rbp), %rdi
	je	.L794
	movq	-704(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	jmp	.L794
.L802:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1361
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L804
.L1372:
	vzeroupper
	jmp	.L804
.L1361:
	vzeroupper
	jmp	.L804
.L1026:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1373
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L815:
	movq	-1440(%rbp), %rdi
	call	_ZN8MmapFileD1Ev
	jmp	.L804
.L813:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1362
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L815
.L1373:
	vzeroupper
	jmp	.L815
.L1363:
	vzeroupper
	jmp	.L826
.L1362:
	vzeroupper
	jmp	.L815
.L945:
	vzeroupper
	call	__cxa_begin_catch@PLT
	call	__cxa_end_catch@PLT
	jmp	.L946
.L902:
	movq	-688(%rbp), %rdi
	cmpq	-1248(%rbp), %rdi
	je	.L1367
	movq	-672(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L904:
	movq	-800(%rbp), %r14
	movq	-792(%rbp), %r12
	movq	%r14, %r13
.L1034:
	cmpq	%r13, %r12
	je	.L1478
	movq	0(%r13), %rdi
	leaq	16(%r13), %rax
	cmpq	%rax, %rdi
	je	.L1035
	movq	16(%r13), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L1035:
	addq	$40, %r13
	jmp	.L1034
.L1478:
	movq	-784(%rbp), %rsi
	subq	%r14, %rsi
	testq	%r14, %r14
	je	.L1038
	movq	%r14, %rdi
	call	_ZdlPvm@PLT
.L1038:
	movq	-928(%rbp), %rdi
	movq	-912(%rbp), %rsi
	subq	%rdi, %rsi
	testq	%rdi, %rdi
	je	.L1040
	call	_ZdlPvm@PLT
.L1040:
	movq	-848(%rbp), %rdi
	movq	-832(%rbp), %rsi
	subq	%rdi, %rsi
	testq	%rdi, %rdi
	jne	.L1479
.L849:
	movq	-1400(%rbp), %rdi
	call	_ZNSt6vectorIS_IiSaIiEESaIS1_EED1Ev
.L1042:
	movq	-1456(%rbp), %rdi
	call	_ZN8MmapFileD1Ev
	jmp	.L826
.L1479:
	call	_ZdlPvm@PLT
	jmp	.L849
.L1020:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L1370
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L720
.L659:
	movq	-752(%rbp), %rdi
	cmpq	-1272(%rbp), %rdi
	je	.L1353
	movq	-736(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L1046
.L693:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L1355
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L695
.L1353:
	vzeroupper
	jmp	.L1046
.L1018:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L1369
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L695
.L668:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L1354
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L670
.L1369:
	vzeroupper
	jmp	.L695
.L1370:
	vzeroupper
	jmp	.L720
.L1355:
	vzeroupper
	jmp	.L695
.L1016:
	movq	-656(%rbp), %rdi
	cmpq	%rbx, %rdi
	je	.L1368
	movq	-640(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L670
.L1354:
	vzeroupper
	jmp	.L670
.L1368:
	vzeroupper
	jmp	.L670
.L1022:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1371
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L751
.L1030:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1375
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L904
.L749:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1357
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L751
.L1375:
	vzeroupper
	jmp	.L904
.L847:
	movq	-848(%rbp), %rdi
	movq	-832(%rbp), %rsi
	subq	%rdi, %rsi
	testq	%rdi, %rdi
	je	.L1365
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L849
.L1028:
	movq	-592(%rbp), %rdi
	cmpq	-1240(%rbp), %rdi
	je	.L1374
	movq	-576(%rbp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L826
.L1365:
	vzeroupper
	jmp	.L849
.L1374:
	vzeroupper
	jmp	.L826
.L765:
	movq	-896(%rbp), %rdi
	movq	-880(%rbp), %rsi
	subq	%rdi, %rsi
	testq	%rdi, %rdi
	je	.L1358
	vzeroupper
	call	_ZdlPvm@PLT
	jmp	.L767
.L1032:
	movq	%r15, %rdi
	vzeroupper
	call	_ZNSt14basic_ofstreamIcSt11char_traitsIcEED1Ev@PLT
.L912:
	movq	-688(%rbp), %rdi
	cmpq	-1248(%rbp), %rdi
	je	.L904
	movq	-672(%rbp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
	jmp	.L904
.L1358:
	vzeroupper
	jmp	.L767
.L918:
	vzeroupper
	call	__cxa_begin_catch@PLT
	call	__cxa_end_catch@PLT
	jmp	.L919
.L909:
	movq	-1256(%rbp), %rdi
	vzeroupper
	call	_ZNSt13basic_filebufIcSt11char_traitsIcEED1Ev@PLT
.L910:
	movq	8+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rax
	movq	16+_ZTTSt14basic_ofstreamIcSt11char_traitsIcEE(%rip), %rcx
	movq	%rax, -592(%rbp)
	movq	-24(%rax), %rax
	movq	%rcx, -592(%rbp,%rax)
.L911:
	movq	-1336(%rbp), %rdi
	leaq	16+_ZTVSt9basic_iosIcSt11char_traitsIcEE(%rip), %rax
	movq	%rax, -344(%rbp)
	call	_ZNSt8ios_baseD2Ev@PLT
	jmp	.L912
.L1371:
	vzeroupper
	jmp	.L751
.L1357:
	vzeroupper
	jmp	.L751
.L1367:
	vzeroupper
	jmp	.L904
	.cfi_endproc
.LFE3923:
	.section	.gcc_except_table
	.align 4
.LLSDAC3923:
	.byte	0xff
	.byte	0x9b
	.uleb128 .LLSDATTC3923-.LLSDATTDC3923
.LLSDATTDC3923:
	.byte	0x1
	.uleb128 .LLSDACSEC3923-.LLSDACSBC3923
.LLSDACSBC3923:
	.uleb128 .LEHB130-.LCOLDB57
	.uleb128 .LEHE130-.LEHB130
	.uleb128 0
	.uleb128 0
.LLSDACSEC3923:
	.byte	0x1
	.byte	0
	.align 4
	.long	0

.LLSDATTC3923:
	.section	.text.unlikely
	.text
	.size	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_, .-_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_
	.section	.text.unlikely
	.size	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_.cold, .-_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_.cold
.LCOLDE57:
	.text
.LHOTE57:
	.section	.rodata.str1.1
.LC58:
	.string	"."
.LC59:
	.string	"Usage: "
.LC60:
	.string	" <gendb_dir> [results_dir]"
	.section	.text.unlikely
.LCOLDB61:
	.section	.text.startup,"ax",@progbits
.LHOTB61:
	.p2align 4
	.globl	main
	.type	main, @function
main:
.LFB3968:
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDA3968
	endbr64
	pushq	%r13
	.cfi_def_cfa_offset 16
	.cfi_offset 13, -16
	pushq	%r12
	.cfi_def_cfa_offset 24
	.cfi_offset 12, -24
	pushq	%rbp
	.cfi_def_cfa_offset 32
	.cfi_offset 6, -32
	movq	%rsi, %rbp
	pushq	%rbx
	.cfi_def_cfa_offset 40
	.cfi_offset 3, -40
	subq	$104, %rsp
	.cfi_def_cfa_offset 144
	movq	%fs:40, %rax
	movq	%rax, 88(%rsp)
	xorl	%eax, %eax
	cmpl	$1, %edi
	jle	.L1501
	movq	8(%rsi), %rsi
	leaq	15(%rsp), %r13
	leaq	16(%rsp), %r12
	movl	%edi, %ebx
	movq	%r13, %rdx
	movq	%r12, %rdi
.LEHB131:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC1IS3_EEPKcRKS3_
.LEHE131:
	leaq	.LC58(%rip), %rsi
	cmpl	$2, %ebx
	je	.L1486
	movq	16(%rbp), %rsi
.L1486:
	leaq	48(%rsp), %rbp
	movq	%r13, %rdx
	movq	%rbp, %rdi
.LEHB132:
	call	_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEC1IS3_EEPKcRKS3_
.LEHE132:
	movq	%rbp, %rsi
	movq	%r12, %rdi
.LEHB133:
	call	_Z6run_q4RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6_
.LEHE133:
	movq	48(%rsp), %rdi
	leaq	64(%rsp), %rax
	cmpq	%rax, %rdi
	je	.L1487
	movq	64(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L1487:
	movq	16(%rsp), %rdi
	leaq	32(%rsp), %rax
	cmpq	%rax, %rdi
	je	.L1488
	movq	32(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L1488:
	xorl	%eax, %eax
.L1480:
	movq	88(%rsp), %rdx
	subq	%fs:40, %rdx
	jne	.L1502
	addq	$104, %rsp
	.cfi_remember_state
	.cfi_def_cfa_offset 40
	popq	%rbx
	.cfi_def_cfa_offset 32
	popq	%rbp
	.cfi_def_cfa_offset 24
	popq	%r12
	.cfi_def_cfa_offset 16
	popq	%r13
	.cfi_def_cfa_offset 8
	ret
.L1501:
	.cfi_restore_state
	leaq	_ZSt4cerr(%rip), %r12
	movl	$7, %edx
	movq	%r12, %rdi
	leaq	.LC59(%rip), %rsi
.LEHB134:
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	0(%rbp), %rsi
	movq	%r12, %rdi
	call	_ZStlsISt11char_traitsIcEERSt13basic_ostreamIcT_ES5_PKc@PLT
	movq	%rax, %rbp
	movl	$26, %edx
	leaq	.LC60(%rip), %rsi
	movq	%rax, %rdi
	call	_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l@PLT
	movq	0(%rbp), %rax
	movq	-24(%rax), %rax
	movq	240(%rbp,%rax), %r12
	testq	%r12, %r12
	je	.L1503
	cmpb	$0, 56(%r12)
	je	.L1483
	movsbl	67(%r12), %esi
.L1484:
	movq	%rbp, %rdi
	call	_ZNSo3putEc@PLT
	movq	%rax, %rdi
	call	_ZNSo5flushEv@PLT
	movl	$1, %eax
	jmp	.L1480
.L1483:
	movq	%r12, %rdi
	call	_ZNKSt5ctypeIcE13_M_widen_initEv@PLT
	movq	(%r12), %rax
	leaq	_ZNKSt5ctypeIcE8do_widenEc(%rip), %rdx
	movq	48(%rax), %rax
	movl	$10, %esi
	cmpq	%rdx, %rax
	je	.L1484
	movl	$10, %esi
	movq	%r12, %rdi
	call	*%rax
	movsbl	%al, %esi
	jmp	.L1484
.L1502:
	call	__stack_chk_fail@PLT
.L1503:
	call	_ZSt16__throw_bad_castv@PLT
.LEHE134:
.L1497:
	endbr64
	movq	%rax, %rbp
	jmp	.L1489
.L1496:
	endbr64
	movq	%rax, %rbp
	vzeroupper
	jmp	.L1491
	.section	.gcc_except_table
.LLSDA3968:
	.byte	0xff
	.byte	0xff
	.byte	0x1
	.uleb128 .LLSDACSE3968-.LLSDACSB3968
.LLSDACSB3968:
	.uleb128 .LEHB131-.LFB3968
	.uleb128 .LEHE131-.LEHB131
	.uleb128 0
	.uleb128 0
	.uleb128 .LEHB132-.LFB3968
	.uleb128 .LEHE132-.LEHB132
	.uleb128 .L1496-.LFB3968
	.uleb128 0
	.uleb128 .LEHB133-.LFB3968
	.uleb128 .LEHE133-.LEHB133
	.uleb128 .L1497-.LFB3968
	.uleb128 0
	.uleb128 .LEHB134-.LFB3968
	.uleb128 .LEHE134-.LEHB134
	.uleb128 0
	.uleb128 0
.LLSDACSE3968:
	.section	.text.startup
	.cfi_endproc
	.section	.text.unlikely
	.cfi_startproc
	.cfi_personality 0x9b,DW.ref.__gxx_personality_v0
	.cfi_lsda 0x1b,.LLSDAC3968
	.type	main.cold, @function
main.cold:
.LFSB3968:
.L1489:
	.cfi_def_cfa_offset 144
	.cfi_offset 3, -40
	.cfi_offset 6, -32
	.cfi_offset 12, -24
	.cfi_offset 13, -16
	movq	48(%rsp), %rdi
	leaq	64(%rsp), %rax
	cmpq	%rax, %rdi
	je	.L1499
	movq	64(%rsp), %rax
	leaq	1(%rax), %rsi
	vzeroupper
	call	_ZdlPvm@PLT
.L1491:
	movq	16(%rsp), %rdi
	leaq	32(%rsp), %rax
	cmpq	%rax, %rdi
	je	.L1492
	movq	32(%rsp), %rax
	leaq	1(%rax), %rsi
	call	_ZdlPvm@PLT
.L1492:
	movq	%rbp, %rdi
.LEHB135:
	call	_Unwind_Resume@PLT
.LEHE135:
.L1499:
	vzeroupper
	jmp	.L1491
	.cfi_endproc
.LFE3968:
	.section	.gcc_except_table
.LLSDAC3968:
	.byte	0xff
	.byte	0xff
	.byte	0x1
	.uleb128 .LLSDACSEC3968-.LLSDACSBC3968
.LLSDACSBC3968:
	.uleb128 .LEHB135-.LCOLDB61
	.uleb128 .LEHE135-.LEHB135
	.uleb128 0
	.uleb128 0
.LLSDACSEC3968:
	.section	.text.unlikely
	.section	.text.startup
	.size	main, .-main
	.section	.text.unlikely
	.size	main.cold, .-main.cold
.LCOLDE61:
	.section	.text.startup
.LHOTE61:
	.p2align 4
	.type	_GLOBAL__sub_I__Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE, @function
_GLOBAL__sub_I__Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE:
.LFB5182:
	.cfi_startproc
	endbr64
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	leaq	_ZStL8__ioinit(%rip), %rbp
	movq	%rbp, %rdi
	call	_ZNSt8ios_base4InitC1Ev@PLT
	movq	_ZNSt8ios_base4InitD1Ev@GOTPCREL(%rip), %rdi
	movq	%rbp, %rsi
	leaq	__dso_handle(%rip), %rdx
	popq	%rbp
	.cfi_def_cfa_offset 8
	jmp	__cxa_atexit@PLT
	.cfi_endproc
.LFE5182:
	.size	_GLOBAL__sub_I__Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE, .-_GLOBAL__sub_I__Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.section	.init_array,"aw"
	.align 8
	.quad	_GLOBAL__sub_I__Z15load_dictionaryRKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
	.local	_ZStL8__ioinit
	.comm	_ZStL8__ioinit,1,1
	.section	.rodata.cst4,"aM",@progbits,4
	.align 4
.LC3:
	.long	-8582
	.align 4
.LC4:
	.long	91
	.set	.LC5,.LC37
	.section	.rodata.cst8,"aM",@progbits,8
	.align 8
.LC20:
	.long	0
	.long	1093567616
	.section	.rodata.cst32,"aM",@progbits,32
	.align 32
.LC37:
	.quad	1
	.quad	4
	.quad	7
	.quad	0
	.align 32
.LC38:
	.quad	0
	.quad	1
	.quad	2
	.quad	6
	.align 32
.LC39:
	.quad	0
	.quad	3
	.quad	6
	.quad	0
	.align 32
.LC40:
	.quad	0
	.quad	1
	.quad	2
	.quad	5
	.set	.LC55,.LC39+8
	.section	.data.rel.ro,"aw"
	.align 8
.LC56:
	.quad	_ZTVSt14basic_ofstreamIcSt11char_traitsIcEE+24
	.hidden	DW.ref.__gxx_personality_v0
	.weak	DW.ref.__gxx_personality_v0
	.section	.data.rel.local.DW.ref.__gxx_personality_v0,"awG",@progbits,DW.ref.__gxx_personality_v0,comdat
	.align 8
	.type	DW.ref.__gxx_personality_v0, @object
	.size	DW.ref.__gxx_personality_v0, 8
DW.ref.__gxx_personality_v0:
	.quad	__gxx_personality_v0
	.hidden	__dso_handle
	.ident	"GCC: (Ubuntu 11.4.0-1ubuntu1~22.04.2) 11.4.0"
	.section	.note.GNU-stack,"",@progbits
	.section	.note.gnu.property,"a"
	.align 8
	.long	1f - 0f
	.long	4f - 1f
	.long	5
0:
	.string	"GNU"
1:
	.align 8
	.long	0xc0000002
	.long	3f - 2f
2:
	.long	0x3
3:
	.align 8
4:
