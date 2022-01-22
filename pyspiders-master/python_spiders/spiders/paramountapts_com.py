# -*- coding: utf-8 -*-

import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import math
import requests


class ParamountaptsComSpider(scrapy.Spider):
    name = "paramountapts_com"
    allowed_domains = ["paramountapts.com"]
    start_urls = ['https://www.paramountapts.com/locations/downtown',
                  'https://www.paramountapts.com/locations/ottawaeast',
                  'https://www.paramountapts.com/locations/ottawawest',
                  "https://www.paramountapts.com/locations/ottawasouth"]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_request(self, response):
        yield Request("https://luxuryrentalsuites.ca/lrs/advanced-search/?filter_search_action%5B%5D=furnished&adv6_search_tab=furnished&term_id=9&filter_search_type%5B%5D=&advanced_city=&advanced_area=&property-id=&bedrooms=&bathrooms=&pets-allowed=&price_low_9=1500&price_max_9=30000&available-date=&submit=Search+Properties",
                      method="GET",
                      callback=self.parse,
                      body='',
                      )

    def parse(self, response):
        for appartment in response.css("div.property-item"):
            url = "https://www.paramountapts.com" + \
                appartment.css("div.prop-title > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

     # 2. SCRAPING level 2
    def populate_item(self, response):
        title = response.css(
            '#skrollr-body > div.banner.section.professionals > div > div.property-information > h1::text').get()

        feats = response.css(
            '#skrollr-body > div.content.content-2col.section > div > div.content-body > div.prop-deets > div.property-information-amenities > div > div:nth-child(1) > div:nth-child(2) > ul > li')

        parking = None
        pool = None
        balcony = None
        washing_machine = True
        try:
            for item in feats:
                if "parking" in item.css("li::text").get():
                    parking = True
                elif "swimming pool" in item.css("li::text").get():
                    pool = True
                elif "balconies" in item.css("li::text").get():
                    balcony = True
                elif "laundry" in item.css("li::text").get():
                    washing_machine = True
        except:
            pass

        description = response.css(
            "#skrollr-body > div.content.content-2col.section > div > div.content-body > div.prop-deets > div.property-information-highlights > div > p > span::text").extract()

        images = response.css(
            '#skrollr-body > div.content.content-2col.section > div > div.aside > div.prop-thumbs.clearfix > div > a::attr(href)').extract()

        for i in range(len(images)):
            images[i] = "https://www.paramountapts.com/" + images[i]

        coords = response.css("script:contains('LatLng')").get().split(
            "LatLng(")[1].split(")")[0]
        lat = coords.split(',')[0]
        lng = coords.split(',')[1]

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        lisitngs = response.css(
            '#skrollr-body > div.content.content-2col.section > div > div.content-body > div.prop-deets > div.property-information-types > div > table > tbody')

        rent_array = lisitngs.css(
            'tr:nth-child(2) > td::text').extract()[1:]

        space_array = lisitngs.css(
            'tr:last-child > td::text').extract()[1:]

        bathrooms_array = lisitngs.css(
            'tr:nth-child(4) > td::text').extract()[1:]

        rooms_array = lisitngs.css(
            'tr:nth-child(1) > td > span::text').extract()[1:]
        if rooms_array == []:
            rooms_array = lisitngs.css(
                'tr:nth-child(1) > td ::text').extract()[1:]

        for i in range(0, len(rent_array)):
            item_loader = ListingLoader(response=response)

            rent = rent_array[i].split("$")[-1]

            if rent.strip() is None:
                pass

            space = space_array[i].strip().split(
                " ")[-1].split("-")[-1].replace('*', "").replace('$', "")

            if "," in space:
                space = space.split(",")
                space = space[0]+space[1]
                space = int(space)

            bathrooms = bathrooms_array[i].strip().split('-')[-1]
            bathrooms = math.ceil(float(bathrooms))

            rooms = rooms_array[i].strip()

            if "Bachelor" in rooms:
                rooms = 1
            else:
                rooms = rooms[0]

            try:
                int(rent)
            except:
                return

            # MetaData
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            # item_loader.add_value(
            #     "external_id", "{}".format(id.split("=")[-1].strip()))
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)

            # Property Details
            item_loader.add_value("property_type", "apartment")
            item_loader.add_value("square_meters", int(space))
            item_loader.add_value("room_count", rooms)
            item_loader.add_value("bathroom_count", bathrooms)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            # item_loader.add_value("available_date", avaialble_date)
            item_loader.add_value("parking", parking)
            item_loader.add_value("swimming_pool", pool)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("washing_machine", washing_machine)

            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

            # Images
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            # Monetary Status
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "CAD")

            # LandLord Details
            # item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_name", "Paramount Properties")
            item_loader.add_value("landlord_phone", "613-565-8000")

            yield item_loader.load_item()
