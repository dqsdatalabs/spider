# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class AcropoliImmobiliareSpider(Spider):
    name = 'acropoliimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.acropoliimmobiliare.it"]
    start_urls = ["https://ricerca.acropoliimmobiliare.it/ita/immobili?order_by=&seo=&rental=1&property_type_id=1001&property_subtype_id=&city_id=&district_id=&price_max=&code="]

    def parse(self, response):
        for url in response.css("a.detail::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title_wrapper_data = response.css("div.wrapper-title::text").getall()
        title_lowered = title_wrapper_data[0].lower()
        if (
            ("commerciale" in title_lowered) 
            or ("ufficio" in title_lowered) 
            or ("magazzino" in title_lowered) 
            or ("box" in title_lowered) 
            or ("auto" in title_lowered) 
            or ("negozio" in title_lowered) 
            or ("vendita" in title_lowered) ):
            return

        title = title_wrapper_data[0].strip()
        rent = title_wrapper_data[1].strip()
        property_type = "apartment"

        description = response.css("p.description::text").getall()
        description = " ".join(description)
        square_meters = response.css("span[title='MQ'] + b::text").get()
        room_count = response.css("span[title='Locali'] + b::text").get()
        energy_label = response.css("span[title='Classe Energ.'] + b::text").get()

        images_to_add = []
        images = response.css("div.sl::attr(style)").getall()
        for image in images:
            image_src = image.split("background-image: url(")[1].split(");")[0]
            images_to_add.append(image_src)

        landlord_phone = "010.562546"
        landlord_name = "acropoliimmobiliare"
        landlord_email = "info@acropoliimmobiliare.it"

        map_script = response.css("div#tab-map script::text").get()
        location_list = re.findall("new google.maps.LatLng\(([0-9]+\.[0-9]+,[0-9]+\.[0-9]+)\);", map_script)[0]
        latitude = location_list.split(",")[0]
        longitude = location_list.split(",")[1]

        external_id = re.findall("RIFERIMENTO ([0-9]+)", description)
        if(external_id):
            external_id = external_id[0]
        parking = response.css("div.section:nth-child(5) > ul:nth-child(2) > li:nth-child(3) > b:nth-child(2)::text").get()
        if( not parking ):
            parking = False
        else:
            parking = True

        address = title.split(" a ")[1]
        city = address.split(",")[0]

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("description", description)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("parking", parking)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
       
        yield item_loader.load_item()
