# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class FrankfurtHomecompanySpider(Spider):
    name = 'frankfurt_homecompany_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.frankfurt.homecompany.de"]
    start_urls = ["https://frankfurt.homecompany.de/en/search"]

    def parse(self, response):
        for url in response.css("a.fullLink::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.nextpage::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):

        property_type = "apartment"
        title = response.css("div.content-box h1::text").get()
        rent = response.css("span.amount::attr(data-price)").get()
        currency = "EUR"

        images = response.css("ul.slides li img::attr(src)").getall()
        images = [response.urljoin(image_src) for image_src in images]

        description = response.css("table.detailsTable th:contains('Description') + td p::text").getall()
        description = " ".join(description)
        description = re.sub("\s+"," ", description)

        external_id = response.css("label:contains('Obj.-Nr.') + span::text").get()
        square_meters = response.css("label:contains('Living space ca.') + span::text").get()
        square_meters = square_meters.split(",")[0]

        room_count = response.css("label:contains('Room(s)') + span::text").get()
        room_count = room_count.split(",")[0]

        available_date = response.css("label:contains('Available from') + span::text").get()
        address = response.css("label:contains('District / Location') + span::text").get()
        city = "frankfurt"
        latitude = response.css("div.mapembed::attr(data-lat)").get()
        longitude = response.css("div.mapembed::attr(data-lng)").get()

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        additional_information = response.css("ul.immoDetails li::text").getall()
        additional_information = " ".join(additional_information)
        additional_information = re.sub("\s+"," ", additional_information)

        floor = re.findall("Floor: ([0-9])\.Floor", additional_information)
        if(len(floor) > 0):
            floor = floor[0]
        else:
            floor = None

        floor = response.css("li:contains('Floor:')::text").get()

        furnished = "furnished" in description
        washing_machine = "washing machine" in description

        energy_label = re.findall("Energy class: ([A-Z])", description)
        if(len(energy_label) > 0):
            energy_label = energy_label[0]
        else:
            energy_label = None

        landlord_name = "homecompany"
        landlord_phone = "+49 (0) 69 - 2578790"
        landlord_email = "frankfurt@homecompany.de"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("floor", floor)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
