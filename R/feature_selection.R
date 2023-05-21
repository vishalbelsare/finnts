
select_features <- function(input_data,
                            run_info,
                            train_test_data, 
                            parallel_processing, 
                            date_type, 
                            fast = FALSE) {
  
  input_data <- input_data %>%
    tidyr::drop_na(Target)
  
  if(date_type %in% c("day", "week")) {
    
    votes_needed <- 3
    
    lofo_results <- tibble::tibble()
    
    target_corr_results <- target_corr_fn(input_data, 0.2) %>%
      dplyr::rename(Feature = term) %>%
      dplyr::mutate(Vote = 1, 
                    Auto_Accept = 0) %>%
      dplyr::select(Feature, Vote, Auto_Accept)
    
    boruta_results <- tibble::tibble()
    
  } else {
    
    
    if(!fast) { # run leave one feature out selection
      votes_needed <- 4
      
      lofo_results <- lofo_fn(
        run_info,
        input_data,
        train_test_data,
        parallel_processing) %>%
        dplyr::filter(Imp >= 0) %>%
        dplyr::rename(Feature = LOFO_Var) %>%
        dplyr::mutate(Vote = 1,
                      Auto_Accept = 0) %>%
        dplyr::select(Feature, Vote, Auto_Accept)
    } else{
      votes_needed <- 3
      
      lofo_results <- tibble::tibble()
    }
    
    # correlation to target
    target_corr_results <- target_corr_fn(input_data, 0.5) %>%
      dplyr::rename(Feature = term) %>%
      dplyr::mutate(Vote = 1, 
                    Auto_Accept = 0) %>%
      dplyr::select(Feature, Vote, Auto_Accept)

    # botuta feature selection
    boruta_results <- tibble::tibble(
      Feature = boruta_fn(input_data), 
      Vote = 1, 
      Auto_Accept = 0)
  }
  
  # multicolinearity_results <- tibble::tibble(
  #   Feature = multicolinearity_fn(input_data), 
  #   Vote = 1, 
  #   Auto_Accept = 0)
  
  # random forest feature importance
  vip_rf_results <- vip_rf_fn(input_data) %>%
    dplyr::rename(Feature = Variable) %>%
    dplyr::mutate(Vote = 1, 
                  Auto_Accept = 0) %>%
    dplyr::select(Feature, Vote, Auto_Accept)
  
  # cubist feature importance
  vip_cubist_results <- vip_cubist_fn(input_data) %>%
    dplyr::rename(Feature = Variable) %>%
    dplyr::mutate(Vote = 1, 
                  Auto_Accept = 0) %>%
    dplyr::select(Feature, Vote, Auto_Accept)

  # lasso regression feature importance
  vip_lm_initial <- vip_lm_fn(input_data) 
  
  missing_cols <- setdiff(colnames(input_data %>% 
                                     dplyr::select(-Combo, -Date, -Target)), 
                          vip_lm_initial$Variable)
  
  cat_cols <- input_data %>%
    dplyr::select_if(is.character) %>%
    dplyr::select(tidyselect::contains(missing_cols)) %>%
    colnames()
  
  vip_lm_cols <- input_data %>%
    dplyr::select(tidyselect::contains(cat_cols), 
                  tidyselect::any_of(vip_lm_initial$Variable)) %>%
    colnames()
  
  vip_lm_results <- tibble::tibble(
    Feature = vip_lm_cols, 
    Vote = 1, 
    Auto_Accept = 1
  )

  # consolidate results and create votes
  final_feature_votes <- rbind(
    #multicolinearity_results, 
    target_corr_results, 
    vip_rf_results, 
    vip_cubist_results,
    vip_lm_results, 
    boruta_results, 
    lofo_results) %>%
    dplyr::group_by(Feature) %>%
    dplyr::summarise(Votes = sum(Vote), 
                     Auto_Accept = sum(Auto_Accept)) %>%
    dplyr::arrange(desc(Votes))
  
  fs_list <- final_feature_votes %>%
    #dplyr::filter(Votes >= votes_needed | Auto_Accept > 0) %>%
    dplyr::filter(Votes >= votes_needed) %>%
    dplyr::pull(Feature) %>%
    sort()
  
  data_final <- input_data %>%
    dplyr::select(unique(c("Combo", "Date", "Target", fs_list)))
  
  fs_list_final <- multicolinearity_fn(data_final)
  
  return(fs_list_final)
}


feature_voting_fn <- function(
    multicolinearity_tbl, 
    target_corr_tbl, 
    vip_rf_tbl, 
    vip_lm_tbl, 
    boruta_tbl, 
    lofo_tbl) {
  
  
  final_feature_votes <- lofo_tbl %>%
    dplyr::filter(Imp >= 0) %>%
    dplyr::rename(Feature = LOFO_Var) %>%
    dplyr::mutate(Vote = 1, 
                  Auto_Accept = 0) %>%
    dplyr::select(Feature, Vote, Auto_Accept) %>%
    rbind(
      tibble::tibble(
        Feature = multicolinearity_tbl, 
        Vote = 1, 
        Auto_Accept = 0
      )
    ) %>%
    rbind(
      target_corr_tbl %>%
        dplyr::rename(Feature = term) %>%
        dplyr::mutate(Vote = 1, 
                      Auto_Accept = 0) %>%
        dplyr::select(Feature, Vote, Auto_Accept)
    ) %>%
    rbind(
      vip_rf_tbl %>%
        dplyr::rename(Feature = Variable) %>%
        dplyr::mutate(Vote = 1, 
                      Auto_Accept = 0) %>%
        dplyr::select(Feature, Vote, Auto_Accept)
    ) %>%
    rbind(
      tibble::tibble(Feature = input_data %>%
                       dplyr::select(tidyselect::any_of(vip_lm_tbl$Variable)) %>% 
                       colnames(), 
                     Vote = 1, 
                     Auto_Accept = 1)
    ) %>%
    rbind(
      tibble::tibble(
        Feature = boruta_tbl, 
        Vote = 1, 
        Auto_Accept = 0
      )
    ) %>%
    dplyr::group_by(Feature) %>%
    dplyr::summarise(Votes = sum(Vote), 
                     Auto_Accept = sum(Auto_Accept)) %>%
    dplyr::arrange(desc(Votes))
  
  return(final_feature_votes)
}

multicolinearity_fn <- function(data) {
  recipes::recipe(
    Target ~ .,
    data = data
  ) %>%
    recipes::step_zv(recipes::all_predictors()) %>%
    recipes::step_corr(recipes::all_numeric_predictors(), threshold = .9) %>%
    recipes::prep(training = data) %>%
    recipes::bake(data) %>%
    #dplyr::select_if(is.numeric) %>%
    colnames()
}

target_corr_fn <- function(data, 
                           threshold = 0.5) {
  corrr::correlate(data, quiet = TRUE) %>%
    dplyr::filter(abs(Target) > threshold) %>%
    dplyr::select(term, Target)
}

vip_rf_fn <- function(data, 
                      seed = 123) {
  
  rf_mod <- parsnip::rand_forest(mode = "regression", trees = 100) %>% 
    parsnip::set_engine("ranger", importance = "impurity")
  
  rf_recipe <- 
    recipes::recipe(Target ~ ., data = data %>% dplyr::select(-Date))
  
  rf_workflow <- 
    workflows::workflow() %>% 
    workflows::add_model(rf_mod) %>% 
    workflows::add_recipe(rf_recipe)
  
  set.seed(seed)
  
  ranger_vip_fs <- rf_workflow %>% 
    generics::fit(data) %>% 
    workflows::extract_fit_parsnip() %>% 
    vip::vi() %>%
    dplyr::filter(Importance > 0) #%>%
    #dplyr::slice(1:round((length(colnames(data))*0.3)))
  
  return(ranger_vip_fs)
}

vip_lm_fn <- function(data, 
                      seed = 123) {
  
  model_spec_lm <- parsnip::linear_reg(
    penalty = 0.01
  ) %>%
    parsnip::set_engine("glmnet")
  
  lm_recipe <- 
    recipes::recipe(Target ~ ., data = data %>% dplyr::select(-Date, -Combo)) %>%
    recipes::step_zv(recipes::all_predictors()) %>%
    recipes::step_normalize(recipes::all_numeric_predictors()) %>%
    recipes::step_dummy(recipes::all_nominal())
  
  lm_workflow <- 
    workflows::workflow() %>% 
    workflows::add_model(model_spec_lm) %>% 
    workflows::add_recipe(lm_recipe)
  
  set.seed(seed)
  
  lm_vip_fs <- lm_workflow %>% 
    generics::fit(data) %>% 
    workflows::extract_fit_parsnip() %>% 
    vip::vi() %>%
    dplyr::filter(Importance > 0) #%>%
    #dplyr::slice(1:round((length(colnames(data))*0.3)))
  
  return(lm_vip_fs)
}

vip_cubist_fn <- function(data,
                          seed = 123) {
  
  model_spec_cubist <- parsnip::cubist_rules(
    mode = "regression",
    committees = 25
  ) %>%
    parsnip::set_engine("Cubist")
  
  cubist_recipe <- 
    recipes::recipe(Target ~ ., data = data %>% dplyr::select(-Date)) %>%
    recipes::step_zv(recipes::all_predictors())
  
  cubist_workflow <- 
    workflows::workflow() %>% 
    workflows::add_model(model_spec_cubist) %>% 
    workflows::add_recipe(cubist_recipe)
  
  set.seed(seed)
  
  cubist_vip_fs <- cubist_workflow %>% 
    generics::fit(data) %>% 
    workflows::extract_fit_parsnip() %>% 
    vip::vi() %>%
    dplyr::filter(Importance > 0) 
  
  return(cubist_vip_fs)
}

boruta_fn <- function(data, 
                      iterations = 100) {
  Boruta::Boruta(Target~.,data=data, maxRuns = iterations) %>%
    Boruta::getSelectedAttributes()
}

lofo_fn <- function(run_info, 
                    data, 
                    train_test_splits, 
                    parallel_processing, 
                    pca = FALSE) {
  
  # parallel run info
  par_info <- finnts:::par_start(
    run_info = run_info,
    parallel_processing = parallel_processing,
    num_cores = NULL,
    task_length = data %>%
      dplyr::select(-Combo, -Target, -Date) %>%
      colnames() %>%
      c("Baseline_Model") %>%
      length()
  )
  
  cl <- par_info$cl
  packages <- par_info$packages
  `%op%` <- par_info$foreach_operator
  
  # submit tasks
  final_results <- foreach::foreach(
    x = data %>%
      dplyr::select(-Combo, -Target, -Date) %>%
      colnames() %>%
      c("Baseline_Model"),
    .combine = "rbind", 
    .packages = packages
  ) %op% {
    
    col <- x
    
    if(col == "Baseline_Model") {
      data_lofo <- data
    } else {
      data_lofo <- data %>%
        dplyr::select(-col)
    }
    
    # get xgboost model set up
    model_spec_xgboost <- parsnip::boost_tree(
      mode = "regression"
    ) %>%
      parsnip::set_engine("xgboost")
    
    recipe_spec_xgboost <- data_lofo %>%
      finnts:::get_recipie_configurable(
        rm_date = "with_adj",
        step_nzv = "zv",
        one_hot = TRUE,
        pca = pca
      )
    
    wflw_spec_tune_xgboost <- finnts:::get_workflow_simple(
      model_spec_xgboost,
      recipe_spec_xgboost
    )
    
    # glmnet model
    recipe_spec_glmnet <- data_lofo %>%
      finnts:::get_recipie_configurable(
        rm_date = "with_adj",
        step_nzv = "zv",
        one_hot = FALSE,
        center_scale = TRUE,
        pca = pca
      )
    
    model_spec_glmnet <- parsnip::linear_reg(
      mode = "regression", 
      penalty = double(1)
    ) %>%
      parsnip::set_engine("glmnet")
    
    wflw_spec_glmnet <- finnts:::get_workflow_simple(
      model_spec_glmnet,
      recipe_spec_glmnet
    )
    
    # cubist model
    recipe_spec_cubist <- data_lofo %>%
      finnts:::get_recipie_configurable(
        rm_date = "with_adj",
        step_nzv = "nzv",
        one_hot = FALSE,
        pca = pca
      )
    
    model_spec_cubist <- parsnip::cubist_rules(
      mode = "regression"
    ) %>%
      parsnip::set_engine("Cubist")
    
    wflw_spec_cubist <- finnts:::get_workflow_simple(
      model_spec_cubist,
      recipe_spec_cubist
    )
    
    # run tests
    test_results <- foreach::foreach(
      x = train_test_splits %>%
        dplyr::filter(Run_Type == "Validation") %>%
        dplyr::pull(Train_Test_ID), 
      .combine = "rbind"
    ) %do% {
      
      id <- x
      
      train_end <- train_test_splits %>%
        dplyr::filter(Train_Test_ID == id) %>%
        dplyr::pull(Train_End)
      
      test_end <- train_test_splits %>%
        dplyr::filter(Train_Test_ID == id) %>%
        dplyr::pull(Test_End)
      
      train_data <- data_lofo %>%
        dplyr::filter(Date <= train_end)
      
      test_data <- data_lofo %>%
        dplyr::filter(Date > train_end, 
                      Date <= test_end)
      
      set.seed(123)
      
      xgb_model_fit <- wflw_spec_tune_xgboost %>%
        generics::fit(train_data)
      
      xgb_fcst <- test_data %>%
        dplyr::bind_cols(
          predict(xgb_model_fit, new_data = test_data)
        ) %>%
        dplyr::mutate(Forecast = .pred,
                      Train_Test_ID = id,
                      LOFO_Var = col) %>%
        dplyr::select(Target, Forecast, Train_Test_ID, LOFO_Var)
      
      set.seed(123)
      
      lr_model_fit <- wflw_spec_glmnet %>%
        generics::fit(train_data)
      
      lr_fcst <- test_data %>%
        dplyr::bind_cols(
          predict(lr_model_fit, new_data = test_data)
        ) %>%
        dplyr::mutate(Forecast = .pred,
                      Train_Test_ID = id,
                      LOFO_Var = col) %>%
        dplyr::select(Target, Forecast, Train_Test_ID, LOFO_Var)
      
      set.seed(123)
      
      cubist_model_fit <- wflw_spec_cubist %>%
        generics::fit(train_data)
      
      cubist_fcst <- test_data %>%
        dplyr::bind_cols(
          predict(cubist_model_fit, new_data = test_data)
        ) %>%
        dplyr::mutate(Forecast = .pred, 
                      Train_Test_ID = id, 
                      LOFO_Var = col) %>%
        dplyr::select(Target, Forecast, Train_Test_ID, LOFO_Var)
      
      return(rbind(xgb_fcst, lr_fcst, cubist_fcst))
    }
    
    final_test_results <- test_results %>%
      dplyr::mutate(SE = (Target - Forecast)^2) %>%
      dplyr::group_by(LOFO_Var) %>%
      dplyr::summarise(RMSE = sqrt(mean(SE, na.rm = TRUE)))
    
    return(final_test_results)
    
  }
  
  finnts:::par_end(cl)
  
  baseline_rmse <- final_results %>%
    dplyr::filter(LOFO_Var == 'Baseline_Model') %>%
    dplyr::pull(RMSE)
  
  return_tbl <- final_results %>%
    dplyr::rename(Var_RMSE = RMSE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(Imp = Var_RMSE - baseline_rmse, 
                  Imp_Norm = max(c(1 - (baseline_rmse / Var_RMSE), 0))) %>%
    dplyr::ungroup()
  
  return(return_tbl)
}


xreg_fn <- function(run_info, 
                    data, 
                    xregs,
                    train_test_splits, 
                    parallel_processing) {
  
  # parallel run info
  par_info <- finnts:::par_start(
    run_info = run_info,
    parallel_processing = parallel_processing,
    num_cores = NULL,
    task_length = c(xregs, "Baseline_Model") %>%
      length()
  )
  
  cl <- par_info$cl
  packages <- par_info$packages
  `%op%` <- par_info$foreach_operator
  
  # submit tasks
  final_results <- foreach::foreach(
    x = c(xregs, "Baseline_Model"),
    .combine = "rbind", 
    .packages = packages
  ) %op% {
    
    col <- x
    
    data_fs <- data %>%
      dplyr::select(-tidyselect::contains(xregs))
    
    if(col != "Baseline_Model") {
      data_fs <- data_fs %>%
        cbind(
          data %>%
            dplyr::select(tidyselect::contains(col))
        )
    }
    
    # get xgboost model set up
    model_spec_xgboost <- parsnip::boost_tree(
      mode = "regression"
    ) %>%
      parsnip::set_engine("xgboost")
    
    recipe_spec_xgboost <- data_fs %>%
      finnts:::get_recipie_configurable(
        rm_date = "with_adj",
        step_nzv = "zv",
        one_hot = TRUE,
        pca = FALSE
      )
    
    wflw_spec_tune_xgboost <- finnts:::get_workflow_simple(
      model_spec_xgboost,
      recipe_spec_xgboost
    )
    
    # glmnet model
    recipe_spec_glmnet <- data_fs %>%
      finnts:::get_recipie_configurable(
        rm_date = "with_adj",
        step_nzv = "zv",
        one_hot = FALSE,
        center_scale = TRUE,
        pca = FALSE
      )
    
    model_spec_glmnet <- parsnip::linear_reg(
      mode = "regression", 
      penalty = double(1)
    ) %>%
      parsnip::set_engine("glmnet")
    
    wflw_spec_glmnet <- finnts:::get_workflow_simple(
      model_spec_glmnet,
      recipe_spec_glmnet
    )
    
    # cubist model
    recipe_spec_cubist <- data_fs %>%
      finnts:::get_recipie_configurable(
        rm_date = "with_adj",
        step_nzv = "nzv",
        one_hot = FALSE,
        pca = FALSE
      )
    
    model_spec_cubist <- parsnip::cubist_rules(
      mode = "regression"
    ) %>%
      parsnip::set_engine("Cubist")
    
    wflw_spec_cubist <- finnts:::get_workflow_simple(
      model_spec_cubist,
      recipe_spec_cubist
    )
    
    # run tests
    test_results <- foreach::foreach(
      x = train_test_splits %>%
        dplyr::filter(Run_Type == "Validation") %>%
        dplyr::pull(Train_Test_ID), 
      .combine = "rbind"
    ) %do% {
      
      id <- x
      
      train_end <- train_test_splits %>%
        dplyr::filter(Train_Test_ID == id) %>%
        dplyr::pull(Train_End)
      
      test_end <- train_test_splits %>%
        dplyr::filter(Train_Test_ID == id) %>%
        dplyr::pull(Test_End)
      
      train_data <- data_fs %>%
        dplyr::filter(Date <= train_end)
      
      test_data <- data_fs %>%
        dplyr::filter(Date > train_end, 
                      Date <= test_end)
      
      set.seed(123)
      
      xgb_model_fit <- wflw_spec_tune_xgboost %>%
        generics::fit(train_data)
      
      xgb_fcst <- test_data %>%
        dplyr::bind_cols(
          predict(xgb_model_fit, new_data = test_data)
        ) %>%
        dplyr::mutate(Forecast = .pred,
                      Train_Test_ID = id,
                      FS_Var = col) %>%
        dplyr::select(Target, Forecast, Train_Test_ID, FS_Var)
      
      set.seed(123)
      
      lr_model_fit <- wflw_spec_glmnet %>%
        generics::fit(train_data)
      
      lr_fcst <- test_data %>%
        dplyr::bind_cols(
          predict(lr_model_fit, new_data = test_data)
        ) %>%
        dplyr::mutate(Forecast = .pred,
                      Train_Test_ID = id,
                      FS_Var = col) %>%
        dplyr::select(Target, Forecast, Train_Test_ID, FS_Var)
      
      set.seed(123)
      
      cubist_model_fit <- wflw_spec_cubist %>%
        generics::fit(train_data)
      
      cubist_fcst <- test_data %>%
        dplyr::bind_cols(
          predict(cubist_model_fit, new_data = test_data)
        ) %>%
        dplyr::mutate(Forecast = .pred, 
                      Train_Test_ID = id, 
                      FS_Var = col) %>%
        dplyr::select(Target, Forecast, Train_Test_ID, FS_Var)
      
      return(rbind(xgb_fcst, lr_fcst, cubist_fcst))
    }
    
    final_test_results <- test_results %>%
      dplyr::mutate(SE = (Target - Forecast)^2) %>%
      dplyr::group_by(FS_Var) %>%
      dplyr::summarise(RMSE = sqrt(mean(SE, na.rm = TRUE)))
    
    return(final_test_results)
    
  }
  
  finnts:::par_end(cl)
  
  baseline_rmse <- final_results %>%
    dplyr::filter(FS_Var == 'Baseline_Model') %>%
    dplyr::pull(RMSE)
  
  return_tbl <- final_results %>%
    dplyr::rename(Var_RMSE = RMSE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(Imp = baseline_rmse - Var_RMSE, 
                  Imp_Norm = max(c(1 - (Var_RMSE / baseline_rmse), 0))) %>%
    dplyr::ungroup()
  
  return(return_tbl)
}