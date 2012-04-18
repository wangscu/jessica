package com.mogujie.jessica.store;

import com.mogujie.jessica.util.Bits;


public class VersionBits implements Bits
{
    int[] _delDocs;
    int _currentVersion;

    public VersionBits(int[] delDocsId, int maxDocId)
    {
        this._delDocs = delDocsId;
        this._currentVersion = maxDocId;
    }

    /**
     * 
     * @return isDeleted true 没有删除 false 已经删除
     * 
     */
    @Override
    public boolean get(int index)
    {
        if (index > _currentVersion)
        {
            return false;
        } else
        {
            int version = _delDocs[index];
            if (version > _currentVersion || version == 0)
            {
                return true;
            } else
            {
                return false;
            }
        }
    }

    @Override
    public int length()
    {
        return _currentVersion;
    }

}
