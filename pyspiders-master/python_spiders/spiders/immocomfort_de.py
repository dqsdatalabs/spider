# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates
from ..loaders import ListingLoader

class ImmocomfortDeSpider(scrapy.Spider):
    name = 'immocomfort_de'
    allowed_domains = ['immocomfort.de']
    start_urls = ['https://www.immocomfort.de/immobilienangebote/mietimmobilien/']  # https not http
    country = 'germany'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'Parkh user'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': ['baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine','waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler',]
    }

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.listing-wrap')
        for rental in rentals:
            title = (rental.css('.entry-title ::text').extract_first()).lower()
            if 'vermietet' not in title and 'tiefgaragenstellplätze' not in title:
                external_link = rental.css('.btn-primary::attr(href)').extract_first()
                yield Request(url=external_link,
                              callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.entry-title::text').extract_first()
        description =((((' '.join(response.css('.wpsight-listing-description p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        description = description.replace('Unsere neue Datenschutzerkl\u00e4rung k\u00f6nnen Sie unter //www.immocomfort.de/datenschutz  einsehen.', '')
        geo = response.css('.wpsight-listing-location meta::attr(content)').extract()
        latitude = geo[0]
        longitude = geo[1]
        zipcode, city, address =extract_location_from_coordinates(longitude, latitude)

        property_type ='apartment'
        info_dict = {}
        listing_details = response.css('.listing-details-detail')
        for detail in listing_details:
            key = (detail.css('.listing-details-label::text').extract_first())
            value = detail.css('.listing-details-value::text').extract_first()
            if key:
                info_dict[key.lower()] = value

        external_id = None
        if 'angebot' in info_dict.keys():
            external_id = info_dict['angebot']

        square_meters = None
        if 'wohnfläche' in info_dict.keys():
            square_meters = info_dict['wohnfläche']
            square_meters = square_meters.replace('\xa0m²','')
            square_meters = (int(extract_number_only(extract_number_only(square_meters))))

        room_count = None
        if 'zimmer' in info_dict.keys():
            room_count = info_dict['zimmer']
            room_count = room_count[0]

        images = response.css('.wpsight-image-background-slider-item::attr(style)').extract()
        images = [(i.replace('background-image:url(',''))[:-1] for i in images]

        rent = None
        if 'kaltmiete' in info_dict.keys():
            rent = info_dict['kaltmiete']
            rent = rent.replace('\xa0€', '')
            rent = int(extract_number_only(extract_number_only(rent)))

        deposit = None
        if 'kaution' in info_dict.keys():
            deposit = info_dict['kaution']
            deposit = deposit.replace('\xa0€', '')
            deposit = int(extract_number_only(extract_number_only(deposit)))
        prepaid_rent = None

        utilities = None
        if 'monatl. nebenkosten' in info_dict.keys():
            utilities = info_dict['monatl. nebenkosten']
            utilities = utilities.replace('\xa0€', '')
            utilities = int(extract_number_only(extract_number_only(utilities)))

        energy_label = None
        if 'energieeffizienzklasse' in info_dict.keys():
            energy_label = info_dict['energieeffizienzklasse']


        furnished = None
        if any(word in description.lower() for word in self.keywords['furnished']):
            furnished = True

        parking = None
        if any(word in description.lower() for word in self.keywords['parking']):
            parking = True

        balcony = None
        if any(word in description.lower() for word in self.keywords['balcony']):
            balcony = True

        terrace = None
        if any(word in description.lower() for word in self.keywords['terrace']):
            terrace = True

        swimming_pool = None
        if any(word in description.lower() for word in self.keywords['swimming_pool']):
            swimming_pool = True

        washing_machine = None
        if any(word in description.lower() for word in self.keywords['washing_machine']):
            washing_machine = True

        dishwasher = None
        if any(word in description.lower() for word in self.keywords['dishwasher']):
            dishwasher = True


        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'immoComfort GmbH') # String
        item_loader.add_value("landlord_phone", '07624-982798') # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
