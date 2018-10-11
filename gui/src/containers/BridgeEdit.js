import React, { Component } from 'react'
import PropTypes from 'prop-types'
import matchRouteAndMapDispatchToProps from 'utils/matchRouteAndMapDispatchToProps'
import Typography from '@material-ui/core/Typography'
import Grid from '@material-ui/core/Grid'
import PaddedCard from 'components/PaddedCard'
import Breadcrumb from 'components/Breadcrumb'
import BreadcrumbItem from 'components/BreadcrumbItem'
import TextField from '@material-ui/core/TextField'
import { Button } from '@material-ui/core'
import { bridgeSelector } from 'selectors'
import { connect } from 'react-redux'
import { withStyles } from '@material-ui/core/styles'
import { fetchBridgeSpec } from 'actions'
import ReactStaticLinkComponent from 'components/ReactStaticLinkComponent'

const styles = theme => ({
  card: {
    marginTop: theme.spacing.unit * 5
  },
  breadcrumb: {
    marginTop: theme.spacing.unit * 5,
    marginBottom: theme.spacing.unit * 5
  }
})

const renderLoading = props => (
  <div>Loading...</div>
)

const renderLoaded = props => (
  <div className={props.classes.card}>
    <PaddedCard>
      <Grid container spacing={24}>
        <Grid item xs={12}>
          <TextField
            label='Name'
            value={props.bridge.name}
            disabled
          />
        </Grid>

        <Grid item xs={12}>
          <TextField
            label='URL'
            value={props.bridge.url}
          />
        </Grid>

        <Grid item xs={12}>
          <TextField
            label='Confirmations'
            value={props.bridge.confirmations}
            inputProps={{min: 0}}
          />
        </Grid>

        <Grid item xs={12}>
          <TextField
            label='Minimum Contract Payment'
            value={props.bridge.minimumContractPayment}
            inputProps={{min: 0}}
          />
        </Grid>

        <Grid item xs={12}>
          <Button variant='contained' color='primary' type='submit'>
            Save Bridge
          </Button>
        </Grid>
      </Grid>
    </PaddedCard>
  </div>
)

const renderDetails = props => props.bridge ? renderLoaded(props) : renderLoading(props)

export class BridgeEdit extends Component {
  componentDidMount () {
    this.props.fetchBridgeSpec(this.props.match.params.bridgeId)
  }

  render() {
    return (
      <Grid container>
        <Grid item xs={12}>
          <Breadcrumb className={this.props.classes.breadcrumb}>
            <BreadcrumbItem href='/'>Dashboard</BreadcrumbItem>
            <BreadcrumbItem>></BreadcrumbItem>
            <BreadcrumbItem href='/bridges'>Bridges</BreadcrumbItem>
            <BreadcrumbItem>></BreadcrumbItem>
            <BreadcrumbItem>{this.props.bridge && this.props.bridge.id}</BreadcrumbItem>
          </Breadcrumb>
        </Grid>
        <Grid item xs={12} md={8} xl={6}>
          <Grid container alignItems='center'>
            <Grid item xs={9}>
              <Typography variant='display2' color='inherit'>
                Bridge Spec Details
              </Typography>
            </Grid>
            <Grid item xs={3}>
              <Grid container justify='flex-end'>
                <Grid item>
                  {this.props.bridge &&
                    <Button
                      variant='outlined'
                      color='primary'
                      component={ReactStaticLinkComponent}
                      to={`/bridges/${this.props.bridge.id}`}
                    >
                      Cancel
                    </Button>
                  }
                </Grid>
              </Grid>
            </Grid>
          </Grid>

          {renderDetails(this.props)}
        </Grid>
      </Grid>
    )
  }
}

BridgeEdit.propTypes = {
  classes: PropTypes.object.isRequired,
  bridge: PropTypes.object
}

const mapStateToProps = (state, ownProps) => {
  return {
    bridge: bridgeSelector(state, ownProps.match.params.bridgeId)
  }
}

export const ConnectedBridgeSpec = connect(
  mapStateToProps,
  matchRouteAndMapDispatchToProps({fetchBridgeSpec})
)(BridgeEdit)

export default withStyles(styles)(ConnectedBridgeSpec)
