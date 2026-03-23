"""
CMS Fetcher - Strapi GraphQL API Client
Fetches content from BusinessABC and SportsABC CMS sources.
Contains all GraphQL queries and response key mappings.
"""

import time
import requests
from typing import Dict, List, Tuple, Optional, Any
from tenacity import retry, stop_after_attempt, wait_exponential


# =============================================================================
# BUSINESSABC GRAPHQL QUERIES
# =============================================================================

BUSINESSABC_QUERIES = {
    "post": """
    query getPosts($pagination: PaginationArg) {
      posts(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            content
            tags
            issued_by
            website_link_title
            website_link_link
            feature_image_title
            published_date
            user_id
            search_index_id
            press_release_subscription_id
            in_review
            is_archived
            image_disclaimer
            locale
            createdAt
            updatedAt
            publishedAt
            feature_image {
              data {
                id
                attributes {
                  name
                  alternativeText
                  caption
                  width
                  height
                  formats
                  hash
                  ext
                  mime
                  size
                  url
                  previewUrl
                  provider
                  provider_metadata
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data {
                  attributes { url }
                }
              }
            }
            post_author {
              data {
                attributes {
                  fullname
                  email
                }
              }
            }
            other_authors {
              data {
                attributes {
                  fullname
                }
              }
            }
            categories {
              data {
                attributes { title }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "city": """
    query getCities($pagination: PaginationArg) {
      cities(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            introduction
            data_and_facts
            administration
            economy
            business_environment
            infrastructure
            technology
            history
            references
            social_wellness_and_human_resources
            city_name
            population_total
            population_year
            government_type
            latitude
            longitude
            height
            region
            districts
            foundation_year
            total_area
            center_area
            grand_city_area
            metropolitam_city_population_total
            main_attraction
            weather_code
            postal_codes
            area_codes
            city_website
            timezone
            major_airports
            gdp
            gdp_year
            mayor_name
            facebook_url
            twitter_url
            youtube_video_url
            youtube_channel_url
            linkedin_url
            instagram_url
            featured
            search_index_id
            createdAt
            updatedAt
            publishedAt
            locale
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                  width
                  height
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                  width
                  height
                }
              }
            }
            map_image {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            avatar_video {
              data {
                attributes {
                  url
                }
              }
            }
            city_images {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data {
                  attributes {
                    url
                  }
                }
              }
            }
            country {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            places {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            companies {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            educations {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            organizations {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "company": """
    query getCompanies($pagination: PaginationArg) {
      companies(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            business_tagline
            key_individual_name
            key_individual_position
            key_individual_gender
            products_and_services
            products_and_services_body
            num_of_employees
            headquarters
            registration_address
            established
            tax_number
            registration_number
            funding_status
            net_incomes
            revenues
            company_currency
            revenue_year
            traded_as
            market_cap
            market_cap_rank
            market_cap_country
            summary
            history
            mission
            vision
            markets_with_interest
            references
            recognition_and_awards
            phone
            whatsapp
            email
            website
            facebook_url
            twitter_url
            instagram_url
            linkedin_url
            youtube_channel_url
            youtube_video_url
            latitude
            longitude
            show_price_status
            price_from
            price_to
            verify_listing
            featured
            ai_generated
            has_store
            search_index_id
            verification_token_transaction_id
            female_percentage
            locale
            createdAt
            updatedAt
            publishedAt
            business_logo {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            headquarters_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            company_images {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data { attributes { url } }
              }
            }
            categories {
              data {
                attributes {
                  title
                }
              }
            }
            industries {
              data {
                attributes {
                  title
                }
              }
            }
            locations {
              data {
                attributes {
                  title
                }
              }
            }
            influencers {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            cities {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "education": """
    query getEducations($pagination: PaginationArg) {
      educations(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            business_tagline
            dean_name
            num_academic_staff
            num_students
            locations
            established
            afiliations
            address
            summary
            history
            courses
            global_mba_rankings
            job_integration_rate
            general_information
            latitude
            longitude
            height
            type
            mainDisciplines
            references
            image_disclaimer
            website
            facebook_url
            instagram_url
            twitter_url
            linkedin_url
            youtube_channel_url
            youtube_video_url
            search_index_id
            featured
            locale
            createdAt
            updatedAt
            publishedAt
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            photo {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data { attributes { url } }
              }
            }
            dean_influencer {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            influencers {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            categories {
              data {
                attributes {
                  title
                }
              }
            }
            cities {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            education_campuses {
              data {
                attributes {
                  title
                  description
                }
              }
            }
            types {
              data {
                attributes {
                  title
                  description
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "investor": """
    query getInvestors($pagination: PaginationArg) {
      investors(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            business_tagline
            registration_number
            funding_status
            num_of_employees
            headquarters
            established
            address
            summary
            history
            investment_criteria
            values
            mission
            portofolio
            references
            website
            facebook_url
            instagram_url
            twitter_url
            linkedin_url
            youtube_channel_url
            youtube_video_url
            search_index_id
            featured
            locale
            createdAt
            updatedAt
            publishedAt
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            photo {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data { attributes { url } }
              }
            }
            investment_type {
              data {
                attributes {
                  title
                }
              }
            }
            types {
              data {
                attributes {
                  title
                }
              }
            }
            industries {
              data {
                attributes {
                  title
                }
              }
            }
            influencers {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            key_peoples {
              id
              name
              position
              gender
              custom_gender
              influencer {
                data {
                  attributes {
                    title
                    slug
                  }
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "place": """
    query getPlaces($pagination: PaginationArg) {
      places(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            place_tagline
            establishment_year
            latitude
            longitude
            height
            summary
            website
            facebook_url
            twitter_url
            youtube_video_url
            youtube_channel_url
            linkedin_url
            instagram_url
            character_id
            voice_id
            agent_id
            createdAt
            updatedAt
            publishedAt
            locale
            place_logo {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            places_images {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            map_image {
              data {
                attributes {
                  url
                }
              }
            }
            avatar_photo {
              data {
                attributes {
                  url
                }
              }
            }
            avatar_video {
              data {
                attributes {
                  url
                }
              }
            }
            thumbnail_video {
              data {
                attributes {
                  url
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data {
                  attributes {
                    url
                  }
                }
              }
            }
            city {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            categories {
              data {
                attributes {
                  title
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "influencer": """
    query getInfluencers($pagination: PaginationArg) {
      influencers(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            business_tagline
            residence
            occupation
            known_for
            accolades
            education
            summary
            biography
            vision
            recognition_and_awards
            references
            website
            facebook_url
            instagram_url
            twitter_url
            linkedin_url
            youtube_channel_url
            youtube_video_url
            search_index_id
            verification_token_transaction_id
            featured
            rank
            has_store
            locale
            createdAt
            updatedAt
            publishedAt
            photo {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data { attributes { url } }
              }
            }
            categories {
              data {
                attributes {
                  title
                }
              }
            }
            companies {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            educations {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            organizations {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            investors {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "knowledgebase": """
    query getKnowledgeBases($pagination: PaginationArg) {
      knowledgeBases(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            headline
            description
            header_description
            createdAt
            updatedAt
            publishedAt
            locale
            feature_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            cover_image {
              data {
                attributes {
                  url
                  alternativeText
                  caption
                }
              }
            }
            thumbnail_video {
              data {
                attributes {
                  url
                }
              }
            }
            cover_video {
              data {
                attributes {
                  url
                }
              }
            }
            seo {
              metaTitle
              metaDescription
              canonicalURL
              metaImage {
                data {
                  attributes {
                    url
                  }
                }
              }
            }
            categories {
              data {
                attributes {
                  title
                  slug
                }
              }
            }
            post_author {
              data {
                attributes {
                  fullname
                }
              }
            }
            applications {
              data {
                id
                attributes {
                  name
                  type
                  maintenance
                  code
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """
}

BUSINESSABC_RESPONSE_KEYS = {
    "post": "posts",
    "city": "cities",
    "company": "companies",
    "education": "educations",
    "investor": "investors",
    "place": "places",
    "influencer": "influencers",
    "knowledgebase": "knowledgeBases"
}


# =============================================================================
# SPORTSABC GRAPHQL QUERIES
# =============================================================================

SPORTSABC_QUERIES = {
    "athletes": """
    query getPlayers($pagination: PaginationArg) {
      players(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            createdAt
            updatedAt
            firstname
            lastname
            fullname
            tagline
            AthleteDescription
            isVerified
            birthdate
            placeOfBirth
            positions
            currentTeam
            number
            website
            widgetYouthCareer
            widgetSeniorCareer
            widgetInternationalCareer
            fullDescription
            career
            internacionalCareer
            stateOfPlay
            personalLife
            legacy
            awardsAndRecognition
            references
            achievements
            publishedAt
            locale
            featureImage {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            heroMedia {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            widgetPicture {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            nationality {
              data {
                id
                attributes {
                  country
                  countryCode
                }
              }
            }
            sport {
              data {
                id
                attributes {
                  title
                  slug
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "teams": """
    query getTeams($pagination: PaginationArg) {
      teams(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            createdAt
            updatedAt
            fullDescription
            history
            ownership
            award
            references
            widgetFullName
            widgetNickname
            countries
            widgetHomeStadium
            widgetFounded
            founded
            owner
            widgetHeadCoach
            widgetCaptain
            website
            publishedAt
            locale
            widgetPicture {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            nationality {
              data {
                id
                attributes {
                  country
                  countryCode
                }
              }
            }
            sport {
              data {
                id
                attributes {
                  title
                  slug
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "stadiums": """
    query getStadiums($pagination: PaginationArg) {
      stadiums(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            createdAt
            updatedAt
            fullDescription
            history
            structure
            uses
            references
            widgetFullName
            location
            owner
            widgetOperator
            capacity
            widgetSurfaceType
            widgetOpenedDate
            size
            widgetConstructionCost
            website
            publishedAt
            locale
            widgetPicture {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            nationality {
              data {
                id
                attributes {
                  country
                  countryCode
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "sports": """
    query getSports($pagination: PaginationArg) {
      sports(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            createdAt
            updatedAt
            fullDescription
            keyType
            competitions
            references
            widgetSupremeAuthority
            widgetOrigin
            widgetType
            widgetVenue
            widgetTeamMembers
            widgetMixedSex
            WidgetEquipment
            firstPlayed
            focus
            publishedAt
            locale
            widgetPicture {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "federations": """
    query getFederations($pagination: PaginationArg) {
      federations(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            createdAt
            updatedAt
            fullDescription
            disciplines
            membership
            references
            widgetAbbreviation
            widgetFounded
            widgetHeadquarters
            widgetRegionServed
            president
            mainOrgan
            widgetAffiliations
            phone
            fax
            email
            website
            headquarters
            lightMembership
            established
            publishedAt
            locale
            imageFit
            type
            widgetPicture {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            sport {
              data {
                id
                attributes {
                  title
                  slug
                }
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "nationality": """
    query getNationalities($pagination: PaginationArg) {
      nationalities(pagination: $pagination) {
        data {
          id
          attributes {
            createdAt
            updatedAt
            country
            countryCode
            publishedAt
            locale
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """,

    "post": """
    query getPosts($pagination: PaginationArg) {
      posts(pagination: $pagination) {
        data {
          id
          attributes {
            title
            slug
            description
            createdAt
            updatedAt
            content
            tags
            publishedAt
            locale
            featureImage {
              data {
                attributes {
                  url
                  alternativeText
                }
              }
            }
            author {
              data {
                id
              }
            }
            categories {
              data {
                id
              }
            }
            applications {
              data {
                id
              }
            }
          }
        }
        meta {
          pagination {
            total
            page
            pageSize
            pageCount
          }
        }
      }
    }
    """
}

SPORTSABC_RESPONSE_KEYS = {
    "athletes": "players",
    "teams": "teams",
    "stadiums": "stadiums",
    "sports": "sports",
    "federations": "federations",
    "nationality": "nationalities",
    "post": "posts"
}



# =============================================================================
# CMS FETCHER CLASS
# =============================================================================

class CMSFetcher:
    """
    Client for fetching data from Strapi GraphQL APIs.
    Supports both BusinessABC and SportsABC CMS sources.
    """
    
    def __init__(self, graphql_url: str, bearer_token: str, logger=None):
        """
        Initialize the CMS fetcher.
        
        Args:
            graphql_url: GraphQL endpoint URL
            bearer_token: API bearer token
            logger: Optional logger instance
        """
        self.graphql_url = graphql_url
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json'
        })
    
    def _log(self, level: str, message: str):
        """Log a message if logger is available."""
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    def fetch_page(self, query: str, response_key: str, page: int, 
                   page_size: int = 25) -> Dict[str, Any]:
        """
        Fetch a single page using the provided query.
        
        Args:
            query: GraphQL query string
            response_key: Key to extract data from response
            page: Page number to fetch
            page_size: Number of records per page
        
        Returns:
            Dictionary with 'data' and 'meta' keys
        """
        variables = {
            'pagination': {
                'page': page,
                'pageSize': page_size
            }
        }
        
        payload = {
            'query': query,
            'variables': variables
        }
        
        try:
            response = self.session.post(
                self.graphql_url,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if 'errors' in data:
                error_messages = [err.get('message', 'Unknown') for err in data['errors']]
                raise Exception(f"GraphQL errors: {', '.join(error_messages)}")
            
            return data['data'][response_key]
            
        except requests.exceptions.RequestException as e:
            self._log('error', f"API request failed: {str(e)}")
            raise
    
    def fetch_all_pages(self, query: str, response_key: str, page_size: int = 25,
                       start_page: int = 1, max_records: Optional[int] = None,
                       delay: float = 0.5) -> Tuple[List[Dict], Dict]:
        """
        Fetch all pages with optional record limit.
        
        Args:
            query: GraphQL query string
            response_key: Key to extract data from response
            page_size: Records per page
            start_page: Starting page number
            max_records: Maximum records to fetch (None = all)
            delay: Delay between requests in seconds
        
        Returns:
            Tuple of (records list, stats dict)
        """
        all_records = []
        current_page = start_page
        total_pages = None
        consecutive_failures = 0
        stats = {
            'total_fetched': 0,
            'failed_pages': [],
            'start_page': start_page,
            'end_page': None,
            'total_pages': 0,
            'total_available': 0
        }
        
        while True:
            try:
                result = self.fetch_page(query, response_key, current_page, page_size)
                records = result['data']
                pagination = result['meta']['pagination']
                
                if total_pages is None:
                    total_pages = pagination['pageCount']
                    stats['total_pages'] = total_pages
                    stats['total_available'] = pagination['total']
                    self._log('info', f"Total pages: {total_pages}, Total records: {pagination['total']}")
                
                # Apply record limit if specified
                if max_records and len(all_records) + len(records) > max_records:
                    records_to_take = max_records - len(all_records)
                    if records_to_take > 0:
                        all_records.extend(records[:records_to_take])
                        stats['total_fetched'] += records_to_take
                    break
                else:
                    all_records.extend(records)
                    stats['total_fetched'] += len(records)
                
                stats['end_page'] = current_page
                consecutive_failures = 0
                
                self._log('info', f"Page {current_page}/{total_pages}: Fetched {len(records)} records")
                
                if max_records and len(all_records) >= max_records:
                    break
                
                if current_page >= total_pages:
                    break
                
                current_page += 1
                time.sleep(delay)
                
            except Exception as e:
                self._log('error', f"Failed to fetch page {current_page}: {str(e)}")
                stats['failed_pages'].append(current_page)
                consecutive_failures += 1
                current_page += 1
                
                if consecutive_failures >= 3:
                    self._log('error', "Too many consecutive failures, stopping")
                    break
                
                time.sleep(2)
        
        return all_records, stats
    
    def fetch_collection(self, cms_key: str, collection: str, 
                        page_size: int = 25, max_records: Optional[int] = None,
                        delay: float = 0.5) -> Tuple[List[Dict], Dict]:
        """
        Fetch a specific collection from a CMS source.
        
        Args:
            cms_key: CMS identifier ('businessabc' or 'sportsabc')
            collection: Collection type (e.g., 'post', 'city')
            page_size: Records per page
            max_records: Maximum records to fetch
            delay: Delay between requests
        
        Returns:
            Tuple of (records list, stats dict)
        """
        if cms_key == 'businessabc':
            queries = BUSINESSABC_QUERIES
            response_keys = BUSINESSABC_RESPONSE_KEYS
        else:
            queries = SPORTSABC_QUERIES
            response_keys = SPORTSABC_RESPONSE_KEYS
        
        if collection not in queries:
            raise ValueError(f"Unknown collection '{collection}' for CMS '{cms_key}'")
        
        query = queries[collection]
        response_key = response_keys[collection]
        
        return self.fetch_all_pages(
            query, response_key,
            page_size=page_size,
            max_records=max_records,
            delay=delay
        )
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the API connection.
        
        Returns:
            Tuple of (success boolean, message string)
        """
        try:
            # Try to fetch 1 record from posts
            result = self.fetch_page(
                BUSINESSABC_QUERIES.get("post", SPORTSABC_QUERIES.get("post", "")),
                "posts",
                page=1,
                page_size=1
            )
            total = result['meta']['pagination']['total']
            return True, f"Connected successfully - {total} posts available"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"


def create_fetcher(cms_key: str, config) -> CMSFetcher:
    """
    Factory function to create a CMS fetcher for a specific source.
    
    Args:
        cms_key: 'businessabc' or 'sportsabc'
        config: Config object with API credentials
    
    Returns:
        Configured CMSFetcher instance
    """
    if cms_key == 'businessabc':
        return CMSFetcher(config.businessabc_url, config.businessabc_token)
    else:
        return CMSFetcher(config.sportsabc_url, config.sportsabc_token)
