#!/usr/bin/env python
# -*- coding:utf-8 -*-


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class GraceDict(dict):
    """
    Return empty string instead of throw KeyError when key is not exist.

    Friendly for web development.
    """

    def __missing__(self, name):
        # 用于 d[key]形式,没有键的情况
        return ""

    def __getitem__(self, name):
        # 用于 d[key]形式,键值为None的情况
        v = super(GraceDict, self).__getitem__(name)
        if v is not None:
            return v
        else:
            return ""

    def get(self, key, default=""):
        # 用于 d.get(key) 的形式
        if key in self:
            r = "" if self[key] is None else self[key]
            return r
        elif default:
            return default
        else:
            return ""


def is_array(obj):
    return isinstance(obj, tuple) or isinstance(obj, list)
