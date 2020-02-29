-- phpMyAdmin SQL Dump
-- version 4.8.5
-- https://www.phpmyadmin.net/
--
-- 主机： localhost
-- 生成日期： 2020-02-29 21:07:54
-- 服务器版本： 5.7.26
-- PHP 版本： 7.3.4

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- 数据库： `saiorm_test`
--
CREATE DATABASE IF NOT EXISTS `saiorm_test` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `saiorm_test`;

-- --------------------------------------------------------

--
-- 表的结构 `no_prefix`
--

DROP TABLE IF EXISTS `no_prefix`;
CREATE TABLE IF NOT EXISTS `no_prefix` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- 插入之前先把表清空（truncate） `no_prefix`
--

TRUNCATE TABLE `no_prefix`;
-- --------------------------------------------------------

--
-- 表的结构 `prefix_blog`
--

DROP TABLE IF EXISTS `prefix_blog`;
CREATE TABLE IF NOT EXISTS `prefix_blog` (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id` int(10) UNSIGNED NOT NULL,
  `content` text,
  `title` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

--
-- 插入之前先把表清空（truncate） `prefix_blog`
--

TRUNCATE TABLE `prefix_blog`;
--
-- 转存表中的数据 `prefix_blog`
--

INSERT INTO `prefix_blog` (`id`, `user_id`, `content`, `title`) VALUES
(1, 1, 'content aaa', 'aaa'),
(2, 2, 'content bbb', 'bbb'),
(3, 3, 'content ccc', 'ccc'),
(4, 4, 'content ddd', 'ddd');

-- --------------------------------------------------------

--
-- 表的结构 `prefix_login_log`
--

DROP TABLE IF EXISTS `prefix_login_log`;
CREATE TABLE IF NOT EXISTS `prefix_login_log` (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id` int(10) UNSIGNED NOT NULL DEFAULT '0',
  `login_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;

--
-- 插入之前先把表清空（truncate） `prefix_login_log`
--

TRUNCATE TABLE `prefix_login_log`;
--
-- 转存表中的数据 `prefix_login_log`
--

INSERT INTO `prefix_login_log` (`id`, `user_id`, `login_time`) VALUES
(1, 1, '2020-01-01 00:00:00'),
(2, 1, '2020-01-01 10:00:00'),
(3, 2, '2020-01-01 11:00:00'),
(4, 3, '2010-01-02 00:00:00'),
(5, 2, '2020-01-02 07:00:00'),
(6, 4, '2020-01-02 07:00:00');

-- --------------------------------------------------------

--
-- 表的结构 `prefix_user`
--

DROP TABLE IF EXISTS `prefix_user`;
CREATE TABLE IF NOT EXISTS `prefix_user` (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  `login_times` int(10) UNSIGNED NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

--
-- 插入之前先把表清空（truncate） `prefix_user`
--

TRUNCATE TABLE `prefix_user`;
--
-- 转存表中的数据 `prefix_user`
--

INSERT INTO `prefix_user` (`id`, `name`, `phone`, `login_times`) VALUES
(1, 'zhangsan', '13112345678', 111),
(2, 'lisi', '13212345678', 222),
(3, 'wangwu', '13312345678', 333),
(4, 'maliu', '13412345678', 444);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
