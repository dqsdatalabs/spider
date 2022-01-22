# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests
from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class CompagniaimmobiliareSpider(Spider):
    name = 'compagniaimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.compagniaimmobiliare.it"]
    start_urls = ["https://www.compagniaimmobiliare.to/r/annunci/affitto-appartamento-.html?Tipologia[]=1&Motivazione[]=2&Codice=&cf=yes"]

    def parse(self, response):
        for url in response.css("section figure a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.next::attr(onclick)").get()
        if (next_page):
            next_page = next_page.split("paginateAjax('")[1]
            next_page = next_page.split("')")[0]
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):

        data = {}
        data["response"] = response
        data["property_type"] = "apartment"

        data["title"] = response.css("h1.titoloscheda::text").get()
        data["rent"] = response.css("div.box:contains('Prezzo')::text").get()
        data["currency"] = "EUR"

        data["description"] = response.css("p.testo_descrizione::text").get()
        data["room_count"] = response.css("div.box:contains('Locali')::text").get()
        data["bathroom_count"] = response.css("div.box:contains('Bagni')::text").get()
        square_meters = response.css("div.box:contains('Totale mq')::text").get()
        data["square_meters"] = re.findall("([0-9]+)", square_meters)[0]
        data["floor"] = response.css("div.box:contains('Piano')::text").get()

        energy_label = response.css("div.box:contains('Classe energetica :')::text").get()
        if(energy_label):
            energy_label = re.findall(" ([A-Z]) .+", energy_label)
            if(len(energy_label) > 0):
                energy_label = energy_label[0]
            else:
                energy_label = None
        data["energy_label"] = energy_label

        elevator = response.css("div.box:contains('Ascensore')::text").get()
        if(elevator):
            if(re.search("Si", elevator)):
                elevator = True
            else: 
                elevator = False
        data["elevator"] = elevator

        external_id = response.css("div.box:contains('Codice')::text").get()
        data["external_id"] = external_id

        location_script = response.css("script:contains('lgt')::text").get()

        data["latitude"] = re.findall('var lat = "(-?[0-9]+\.[0-9]+)";', location_script)[0]
        data["longitude"] = re.findall('var lgt = "(-?[0-9]+\.[0-9]+)";', location_script)[0]

        data["landlord_name"] = "compagniaimmobiliare"
        data["landlord_phone"] = "011.4081421 "
        data["landlord_email"] = "grugliascocompagniaimmobiliare@gmail.com"

        images_id = response.css("input[name='id']::attr(value)").get()
        images_url = f"https://www.compagniaimmobiliare.to/moduli/realestate/immobili_gallery.php?plan=0&idImmobile={images_id}"

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={data['latitude']},{data['longitude']}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        data["address"] = responseGeocodeData['address']['Match_addr']
        data["city"] = responseGeocodeData['address']['City']

        yield Request(url = images_url, callback = self.get_images, dont_filter = True, meta = {"data": data})

    def get_images(self, response):

        data = response.meta.get("data")
        images = response.css("a.swipebox::attr(href)").getall()


        item_loader = ListingLoader(response=data["response"])

        item_loader.add_value("external_link", data["response"].url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", data["property_type"])
        item_loader.add_value("title", data["title"])
        item_loader.add_value("rent_string", data["rent"])
        item_loader.add_value("currency", data["currency"])
        item_loader.add_value("description", data["description"])
        item_loader.add_value("room_count", data["room_count"])
        item_loader.add_value("bathroom_count", data["bathroom_count"])
        item_loader.add_value("square_meters", data["square_meters"])
        item_loader.add_value("floor", data["floor"])
        item_loader.add_value("energy_label", data["energy_label"])
        item_loader.add_value("elevator", data["elevator"])
        item_loader.add_value("external_id", data["external_id"])
        item_loader.add_value("latitude", data["latitude"])
        item_loader.add_value("longitude", data["longitude"])
        item_loader.add_value("landlord_name", data["landlord_name"])
        item_loader.add_value("landlord_phone", data["landlord_phone"])
        item_loader.add_value("landlord_email", data["landlord_email"])
        item_loader.add_value("images", images)
        item_loader.add_value("address", data["address"])
        item_loader.add_value("city", data["city"])
       
        yield item_loader.load_item()
