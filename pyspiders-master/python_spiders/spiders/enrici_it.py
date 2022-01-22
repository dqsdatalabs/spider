# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class EnriciSpider(Spider):
    name = 'Enrici_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.enrici.it"]
    start_urls = ["https://www.enrici.it/status/affitto/"]

    def parse(self, response):
        for url in response.css("h2 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css("div.table-cell h1::text").get().strip()
        if (("commerciale" in title.lower()) or ("ufficio" in title.lower()) or ("magazzino" in title.lower()) or ("box" in title.lower()) or ("negozio" in title.lower()) ):
            return
        
        property_type = response.css("li.prop_type::text").get().strip()

        if(property_type == "Casa indipendente"):
            property_type = "house"
        else: 
            property_type = "apartment"

        rent = response.css("div.header-right span.item-price::text").get().strip()
        address = response.css("address.property-address::text").get().strip()
        room_count = response.css("li.numero-locali1527590917f5b0d3005bb59f::text").get().strip()
        square_meters = response.css("ul.list-three-col:nth-child(1) > li:nth-child(3)::text").get().strip()
        city = response.css("li.detail-city::text").get().strip()
        zipcode = response.css("li.detail-zip::text").get().strip()
        
        images = response.css("img.size-houzez-imageSize1170_738::attr(src)").getall()
        images_to_add = []
        for image in images: 
            match = re.findall("^(https://www.enrici.it/)*", image)
            if(len(match[0])):
                images_to_add.append(image)


        energy_label = response.css("dl.houzez-energy-table dd::text").get().strip()
        
        landlord_name = "Enrici domain"
        landlord_email = "enrici@enrici.it"
        landlord_phone = "011.6605070"

        external_id = response.css(".riferimento1527595771f5b0d42fb12df8::text").get()
        description = response.css("div#description p::text").getall()
        description = " ".join(description)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("description", description)
       
        yield item_loader.load_item()


