--
-- PostgreSQL database dump
--

-- Dumped from database version 12.2
-- Dumped by pg_dump version 12.2

-- Started on 2020-03-01 19:46:40

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO postgres;

--
-- TOC entry 2847 (class 0 OID 0)
-- Dependencies: 3
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- TOC entry 206 (class 1259 OID 16438)
-- Name: seq_blog; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.seq_blog
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.seq_blog OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 202 (class 1259 OID 16397)
-- Name: prefix_blog; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.prefix_blog (
    id integer DEFAULT nextval('public.seq_blog'::regclass) NOT NULL,
    user_id integer DEFAULT 0 NOT NULL,
    content text,
    title character varying
);


ALTER TABLE public.prefix_blog OWNER TO postgres;

--
-- TOC entry 207 (class 1259 OID 16440)
-- Name: seq_login_log; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.seq_login_log
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.seq_login_log OWNER TO postgres;

--
-- TOC entry 203 (class 1259 OID 16406)
-- Name: prefix_login_log; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.prefix_login_log (
    id integer DEFAULT nextval('public.seq_login_log'::regclass) NOT NULL,
    user_id integer DEFAULT 0 NOT NULL,
    login_time timestamp without time zone
);


ALTER TABLE public.prefix_login_log OWNER TO postgres;

--
-- TOC entry 204 (class 1259 OID 16414)
-- Name: seq_user; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.seq_user
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.seq_user OWNER TO postgres;

--
-- TOC entry 205 (class 1259 OID 16430)
-- Name: prefix_user; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.prefix_user (
    id integer DEFAULT nextval('public.seq_user'::regclass) NOT NULL,
    name character varying,
    phone character varying,
    login_times integer
);


ALTER TABLE public.prefix_user OWNER TO postgres;

--
-- TOC entry 2836 (class 0 OID 16397)
-- Dependencies: 202
-- Data for Name: prefix_blog; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.prefix_blog (id, user_id, content, title) FROM stdin;
1	1	content aaa	aaa
2	2	content bbb	bbb
3	3	content ccc	ccc
4	4	content ddd	ddd
\.


--
-- TOC entry 2837 (class 0 OID 16406)
-- Dependencies: 203
-- Data for Name: prefix_login_log; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.prefix_login_log (id, user_id, login_time) FROM stdin;
1	1	2020-01-01 00:00:00
3	2	2020-01-01 11:00:00
4	3	2010-01-02 00:00:00
5	2	2020-01-02 07:00:00
6	4	2020-01-02 07:00:00
2	1	2020-01-01 10:00:00
\.


--
-- TOC entry 2839 (class 0 OID 16430)
-- Dependencies: 205
-- Data for Name: prefix_user; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.prefix_user (id, name, phone, login_times) FROM stdin;
1	zhangsan	13112345678	111
2	lisi	13212345678	222
3	wangwu	13312345678	333
4	maliu	13412345678	444
\.


--
-- TOC entry 2848 (class 0 OID 0)
-- Dependencies: 206
-- Name: seq_blog; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.seq_blog', 4, true);


--
-- TOC entry 2849 (class 0 OID 0)
-- Dependencies: 207
-- Name: seq_login_log; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.seq_login_log', 6, true);


--
-- TOC entry 2850 (class 0 OID 0)
-- Dependencies: 204
-- Name: seq_user; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.seq_user', 4, true);


--
-- TOC entry 2707 (class 2606 OID 16423)
-- Name: prefix_blog prefix_blog_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.prefix_blog
    ADD CONSTRAINT prefix_blog_pk PRIMARY KEY (id);


--
-- TOC entry 2709 (class 2606 OID 16411)
-- Name: prefix_login_log prefix_login_log_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.prefix_login_log
    ADD CONSTRAINT prefix_login_log_pk PRIMARY KEY (id);


-- Completed on 2020-03-01 19:46:40

--
-- PostgreSQL database dump complete
--

