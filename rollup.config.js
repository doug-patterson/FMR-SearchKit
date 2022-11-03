import styles from "rollup-plugin-styles"
const autoprefixer = require('autoprefixer')
import babel from '@rollup/plugin-babel'

const input = 'react/src/index.js'

let MODE = [
  {
    format: 'cjs'
  },
  {
    format: 'esm'
  },
  {
    format: 'umd'
  }
]

let config = []

MODE.map((m) => {
    var conf = {
        input: input,
        output: {
            name: "fmr-searchkit-react",
            file: `react/dist/index.${m.format}.js`,
            format: m.format,
            exports: "auto"
        },
        external: ["react", /@babel\/runtime/],
        plugins: [
            babel({
                exclude: 'node_modules/**',
                plugins: ['@babel/transform-runtime'],
                babelHelpers: 'runtime'
            }),
            styles({
                postcss: {
                    plugins: [
                        autoprefixer()
                    ]
                }
            })
        ]
    }
    config.push(conf)
})

export default [
  ...config,
]