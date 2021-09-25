# -*- coding: utf-8; -*-

"""\
参考にした参考にしたQiita記事: https://qiita.com/komiya-m/items/c37c9bc308d5294d3260
"""

import sys
import os
import shutil
import random
import atexit

_mkdir_log = []

def commit():
    """現在のディレクトリ構成を確定し、不可逆化する"""
    _mkdir_log.clear()

def rollback():
    """現在のディレクトリ構成を廃棄し、初期化する"""
    for path in reversed(_mkdir_log):
        shutil.rmtree(path, ignore_errors=True)
    commit()

def _emergency_rollback(value: str="予期しない例外が発生した為、ディレクトリ構成を初期化しました。"):
    if _mkdir_log:
        rollback()
        print(value, file=sys.stderr)

def _mkdir(path: str):
    try:
        os.mkdir(path)
        _mkdir_log.append(os.path.realpath(path))
    except FileExistsError:
        _emergency_rollback("既存ディレクトリを検出した為、ディレクトリ構成を初期化しました。")
        raise
    except:
        _emergency_rollback()
        raise

def _symlink_or_copy(src: str, dst: str, copy: bool=False, verbose: int=1):
    try:
        if copy:
            raise NotImplementedError
        os.symlink(os.path.realpath(src), dst)
        if verbose >= 1:
            print("シンボリックリンク作成:", dst)
    except (NotImplementedError, os.error):
        try:
            shutil.copyfile(src, dst)
            if verbose >= 1:
                print("コピー作成:", dst)
        except:
            _emergency_rollback()
            raise
    except:
        _emergency_rollback()
        raise

def _get_base_dir(original_dir: str, base_dir: str=None, basename: str='sprit_data'):
    if base_dir is None:
        base_dir = os.path.join(*os.path.split(original_dir)[:-1], basename)
    return base_dir

def merge(original_dirs: list, base_dir: str=None, sep: str='-', copy: bool=False, verbose: int=1):
    """\
    ディレクトリ同士を結合する

    Parameters
    ----------
    original_dirs: list, array-like object
      結合対象のオリジナルデータディレクトリのパスのリスト
    base_dir: str
      結合ディレクトリ
    """
    try:
        original_dir = original_dirs[0]
        base_dir = _get_base_dir(original_dir, base_dir,
                                 basename=sep.join([os.path.split(p)[-1] for p in original_dirs]))
        _mkdir(base_dir)

        #クラス分のフォルダ名の取得
        dir_lists = os.listdir(original_dir)
        dir_lists = [f for f in dir_lists if os.path.isdir(os.path.join(original_dir, f))]
        original_dir_path = [os.path.join(original_dir, p) for p in dir_lists]
        #num_class = len(dir_lists)

        # クラスフォルダの作成
        for D in dir_lists:
            dst_class_dir_path = os.path.join(base_dir, D)
            _mkdir(dst_class_dir_path)
            # ファイルのシンボリックリンク作成又はコピー
            for original_dir in original_dirs:
                src_class_dir_path = os.path.join(original_dir, D)
                for fname in os.listdir(src_class_dir_path):
                    _symlink_or_copy(
                        src=os.path.join(src_class_dir_path, fname),
                        dst=os.path.join(dst_class_dir_path, fname),
                        copy=copy,
                        verbose=verbose
                    )
    except:
        _emergency_rollback()
        raise

def kfold_sprit(original_dir: str, n_splits: int=5, base_dir: str=None, fold_dir_basename: str='fold_{:0=2}', copy: bool=False, verbose: int=1):
    """\
    k-分割を行う

    Parameters
    ----------
    original_dir: str
      オリジナルデータディレクトリのパス その下に各クラスのディレクトリがある
    n_splits: int
      分割数(k)
    base_dir: str
      分割したデータを格納するディレクトリのパス　そこにディレクトリが作られる
    """
    try:
        base_dir = _get_base_dir(original_dir, base_dir)
        _mkdir(base_dir)

        #クラス分のフォルダ名の取得
        dir_lists = os.listdir(original_dir)
        dir_lists = [f for f in dir_lists if os.path.isdir(os.path.join(original_dir, f))]
        original_dir_path = [os.path.join(original_dir, p) for p in dir_lists]

        fold_dirs = [os.path.join(base_dir, fold_dir_basename).format(i+1) for i in range(n_splits)]
        
        # k箇のholdディレクトリの作成
        for fold_dir_path in fold_dirs:
            _mkdir(fold_dir_path)
        
        for class_dir_basename, path in zip(dir_lists, original_dir_path):
            # ファイル名を取得してシャッフル
            files_class = os.listdir(path)
            random.shuffle(files_class)
            # ファイルのシンボリックリンク作成又はコピー
            for i, fold_dir_path in enumerate(fold_dirs):
                # クラスディレクトリ作成
                dst_class_dir_path = os.path.join(fold_dir_path, class_dir_basename)
                _mkdir(dst_class_dir_path)
                for fname in files_class[i::n_splits]:
                    _symlink_or_copy(
                        src=os.path.join(path, fname),
                        dst=os.path.join(dst_class_dir_path, fname),
                        copy=copy,
                        verbose=verbose
                        )
    except:
        _emergency_rollback()
        raise

def kfold_cross_validation_preprocess(original_dir: str, n_splits: int=5, base_dir: str=None, train_dir_basename: str='train_{:0=2}', validation_dir_basename: str='validation_{:0=2}', copy: bool=False, verbose: int=1):
    """\
    k-分割交差検証の前処理を行う

    Parameters
    ----------
    original_dir: str
      オリジナルデータディレクトリのパス その下に各クラスのディレクトリがある
    n_splits: int
      分割数(k)
    base_dir: str
      分割したデータを格納するディレクトリのパス　そこにディレクトリが作られる
    """
    try:
        base_dir = _get_base_dir(original_dir, base_dir)

        # validation部の作成
        kfold_sprit(
            original_dir,
            n_splits,
            base_dir,
            fold_dir_basename=validation_dir_basename,
            copy=copy,
            verbose=verbose
            )
        
        # train部の作成
        itersplits = range(1, n_splits+1)
        validation_dir_lists = [[os.path.join(base_dir, validation_dir_basename.format(j)) for j in itersplits if i != j] for i in itersplits]
        for i, validation_dirs in enumerate(validation_dir_lists, start=1):
            print(validation_dirs)
            merge(
                validation_dirs,
                os.path.join(base_dir, train_dir_basename).format(i),
                copy=copy,
                verbose=verbose
                )
    except:
        _emergency_rollback()
        raise

def train_test_sprit(original_dir: str, train_size: float=0.8, base_dir: str=None, train_dir_basename: str='train', validation_dir_basename: str='validation', copy=False, verbose=1):
    """\
    データをtrainデータとvalidationデータにシャッフルして分割する

    Parameters
    ----------
    original_dir: str
      オリジナルデータディレクトリのパス その下に各クラスのディレクトリがある
    train_size: float
      trainデータの割合
    base_dir: str
      分割したデータを格納するディレクトリのパス　そこにディレクトリが作られる
    """
    try:
        base_dir = _get_base_dir(original_dir, base_dir)
        
        _mkdir(base_dir)

        #クラス分のディレクトリ名の取得
        dir_lists = os.listdir(original_dir)
        dir_lists = [f for f in dir_lists if os.path.isdir(os.path.join(original_dir, f))]
        original_dir_path = [os.path.join(original_dir, p) for p in dir_lists]

        # ディレクトリの作成(trainとvalidation)
        train_dir = os.path.join(base_dir, train_dir_basename)
        _mkdir(train_dir)
        validation_dir = os.path.join(base_dir, validation_dir_basename)
        _mkdir(validation_dir)

        #クラスディレクトリの作成
        train_dir_path_lists = []
        val_dir_path_lists = []
        for D in dir_lists:
            train_class_dir_path = os.path.join(train_dir, D)
            _mkdir(train_class_dir_path)
            train_dir_path_lists += [train_class_dir_path]
            val_class_dir_path = os.path.join(validation_dir, D)
            _mkdir(val_class_dir_path)
            val_dir_path_lists += [val_class_dir_path]

        for i, path in enumerate(original_dir_path):
            # ファイル名を取得してシャッフル
            files_class = os.listdir(path)
            random.shuffle(files_class)
            # 分割地点のインデックスを取得
            num_bunkatu = int(len(files_class) * train_size)
            # trainへファイルのシンボリックリンク作成又はコピー
            for fname in files_class[:num_bunkatu]:
                _symlink_or_copy(
                    src=os.path.join(path, fname),
                    dst=os.path.join(train_dir_path_lists[i], fname),
                    copy=copy,
                    verbose=verbose
                    )
            # validationへファイルのシンボリックリンク作成又はコピー
            for fname in files_class[num_bunkatu:]:
                _symlink_or_copy(
                    src=os.path.join(path, fname),
                    dst=os.path.join(val_dir_path_lists[i], fname),
                    copy=copy,
                    verbose=verbose
                    )
    except:
        _emergency_rollback()
        raise

_ = atexit.register(rollback)
