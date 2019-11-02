(ns sparkdefinitive.init-settings
  (:require louna.state.settings))

(defn init []
  (do (louna.state.settings/set-local-session)
      (louna.state.settings/set-log-level "ERROR")
      (louna.state.settings/set-base-path "/home/white/IdeaProjects/louna-spark/")))


