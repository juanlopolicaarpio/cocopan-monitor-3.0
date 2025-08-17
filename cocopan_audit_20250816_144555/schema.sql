--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13
-- Dumped by pg_dump version 15.13

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: status_checks; Type: TABLE; Schema: public; Owner: cocopan
--

CREATE TABLE public.status_checks (
    id integer NOT NULL,
    store_id integer,
    is_online boolean NOT NULL,
    checked_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    response_time_ms integer,
    error_message text
);


ALTER TABLE public.status_checks OWNER TO cocopan;

--
-- Name: status_checks_id_seq; Type: SEQUENCE; Schema: public; Owner: cocopan
--

CREATE SEQUENCE public.status_checks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.status_checks_id_seq OWNER TO cocopan;

--
-- Name: status_checks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: cocopan
--

ALTER SEQUENCE public.status_checks_id_seq OWNED BY public.status_checks.id;


--
-- Name: stores; Type: TABLE; Schema: public; Owner: cocopan
--

CREATE TABLE public.stores (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    url text NOT NULL,
    platform character varying(50) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.stores OWNER TO cocopan;

--
-- Name: stores_id_seq; Type: SEQUENCE; Schema: public; Owner: cocopan
--

CREATE SEQUENCE public.stores_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.stores_id_seq OWNER TO cocopan;

--
-- Name: stores_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: cocopan
--

ALTER SEQUENCE public.stores_id_seq OWNED BY public.stores.id;


--
-- Name: summary_reports; Type: TABLE; Schema: public; Owner: cocopan
--

CREATE TABLE public.summary_reports (
    id integer NOT NULL,
    total_stores integer NOT NULL,
    online_stores integer NOT NULL,
    offline_stores integer NOT NULL,
    online_percentage real NOT NULL,
    report_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.summary_reports OWNER TO cocopan;

--
-- Name: summary_reports_id_seq; Type: SEQUENCE; Schema: public; Owner: cocopan
--

CREATE SEQUENCE public.summary_reports_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.summary_reports_id_seq OWNER TO cocopan;

--
-- Name: summary_reports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: cocopan
--

ALTER SEQUENCE public.summary_reports_id_seq OWNED BY public.summary_reports.id;


--
-- Name: status_checks id; Type: DEFAULT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.status_checks ALTER COLUMN id SET DEFAULT nextval('public.status_checks_id_seq'::regclass);


--
-- Name: stores id; Type: DEFAULT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.stores ALTER COLUMN id SET DEFAULT nextval('public.stores_id_seq'::regclass);


--
-- Name: summary_reports id; Type: DEFAULT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.summary_reports ALTER COLUMN id SET DEFAULT nextval('public.summary_reports_id_seq'::regclass);


--
-- Name: status_checks status_checks_pkey; Type: CONSTRAINT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.status_checks
    ADD CONSTRAINT status_checks_pkey PRIMARY KEY (id);


--
-- Name: stores stores_pkey; Type: CONSTRAINT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_pkey PRIMARY KEY (id);


--
-- Name: stores stores_url_key; Type: CONSTRAINT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_url_key UNIQUE (url);


--
-- Name: summary_reports summary_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.summary_reports
    ADD CONSTRAINT summary_reports_pkey PRIMARY KEY (id);


--
-- Name: idx_status_checks_checked_at; Type: INDEX; Schema: public; Owner: cocopan
--

CREATE INDEX idx_status_checks_checked_at ON public.status_checks USING btree (checked_at);


--
-- Name: idx_status_checks_store_id; Type: INDEX; Schema: public; Owner: cocopan
--

CREATE INDEX idx_status_checks_store_id ON public.status_checks USING btree (store_id);


--
-- Name: status_checks status_checks_store_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cocopan
--

ALTER TABLE ONLY public.status_checks
    ADD CONSTRAINT status_checks_store_id_fkey FOREIGN KEY (store_id) REFERENCES public.stores(id);


--
-- PostgreSQL database dump complete
--

