package com.github.andylke.demo.remotechunking;

import java.util.List;

public interface PagingItemReader<T> {

  List<T> readPage(int page, int pageSize);
}
