# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class Hc24_deSpider(Spider):
    name = 'hc24_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.hc24.de"]
    start_urls = ["https://www.hc24.de/en/furnished-accommodation.htm"]
    position = 1

    def parse(self, response):
        for url in response.css("div.thumbnail a.tmplLoad::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.get_city_page, dont_filter = True)
        
    def get_city_page(self, response):
        all_properties_page = response.css("a:contains('Show all properties')::attr(href)").get()
        yield Request(response.urljoin(all_properties_page), callback=self.get_properties_page, dont_filter = True)

    def get_properties_page(self, response):
        for url in response.css("a.detailviewLoad::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

        next_page = response.css("linl[rel='next']::attr(href)").get()
        if(next_page):
            yield response.follow(response.urljoin(next_page), callback=self.get_properties_page, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"
        title = response.css("h4.headline::text").get()

        property_info = response.css("div.quickinfo div.info-with-icon p.data::text").getall()
        rent = property_info[3]
        rent = rent.split(",-")[0]
        currency = "EUR"
        room_count = property_info[1]
        room_count = room_count.split(" ")[0]
        room_count = int(float(room_count))
        square_meters = property_info[2]
        available_date = property_info[0]
        
        description = response.css("p.description-container::text").getall()
        description = " ".join(description)
        floor = response.css("div:contains('Floor') + div::text").get()

        conditions = response.css("div:contains('Conditions') + div::text").get()
        pets_allowed = "pets by arrangement" in conditions

        deposit = response.css("div:contains('Deposit') + div::text").get()
        if(deposit):
            deposit = deposit.split(",-")[0]
        
        feature_list = response.css("div.feature-list div.feature div::text").getall()
        feature_list = " ".join(feature_list)

        furnished = "furnished" in feature_list
        dishwasher = "dish washer" in feature_list
        balcony = "balcony" in feature_list
        washing_machine = "laundry cellar" in feature_list
        elevator = "Aufzug" in feature_list
        parking = "parking" in feature_list

        energy_label = response.css("td:contains('Consumption value') + td").get()
        if(energy_label):
            energy_label = re.findall("\(([A-Z])\)", energy_label)
            if(len(energy_label) > 0):
                energy_label = energy_label[0]
            else:
                energy_label = None

        images = response.css("div.slick-container-detail div.slick-slide img::attr(data-lazy)").getall()
        latitude = response.css("input#mapCenterLat::attr(value)").get()
        longitude = response.css("input#mapCenterLng::attr(value)").get()
        
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        landlord_name = "hc24"
        landlord_phone = response.css("a.contact-phone::text").get()
        landlord_email = response.css("a.contact-mail::text").get()

        if(rent):
            rent = re.findall("([0-9]+)", rent)
            rent = "".join(rent)
        else:
            return
            
        if(deposit):
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)
        else:
            deposit = None

        amenities = response.css("div.feature div::text").getall()
        amenities = " ".join(amenities).lower()


        furnished = "möblier" in description.lower()
        washing_machine = "waschmaschine" in description.lower() or "waschmaschine" in amenities
        terrace = "terrasse" in description.lower() or "terrasse" in amenities
        balcony = "balkon" in description.lower()

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)
        self.position += 1
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("description", description)
        item_loader.add_value("floor", floor)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("parking", parking)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("balcony", balcony)
       
        yield item_loader.load_item()
