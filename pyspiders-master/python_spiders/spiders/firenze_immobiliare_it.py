# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class FirenzeImmobiliareSpider(Spider):
    name = 'firenze_immobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.firenze-immobiliare.it"]
    start_urls = ["https://www.firenze-immobiliare.it/affitto/"]

    def parse(self, response):
        for url in response.css("div.lower-content h3.text-uppercase a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"

        title = response.css("h2.text-capitalize::text").get()
        if (("commerciale" in title.lower()) or ("ufficio" in title.lower()) or ("magazzino" in title.lower()) or ("box" in title.lower()) or ("negozio" in title.lower()) ):
            return

        rent = response.css("div.price::text").get()
        address = response.css("div.location::text").get()
        
        propert_info = response.css("ul.property-info li::text").getall()
        square_meters = propert_info[1].split("Sup. ")[1]
        room_count = propert_info[3].split(" Vani")[0]
        bathroom_count = propert_info[5].split(" Bagni")[0]
        
        energy_label = None
        if( len(propert_info) == 8 ):
            energy_label = propert_info[7].split("Classe ")[1]

        images = response.css("div.items-container div.gallery-item div.image-box figure.image img::attr(data-src)").getall()
        images_to_add = []
        for image in images:
            images_to_add.append("https://" + self.allowed_domains[0] + image) 

        description = response.css("div.property-detail:nth-child(1) > p:nth-child(1)::text").get()
        landlord_name = "Firenze Immobiliare"
        landlord_email = "info@firenze-immobiliare.it"
        landlord_phone = "347 3252911"
        external_id = response.css(".info-table > li:nth-child(1)::text").get()
        
        try:
            city = address.split("-")[1].strip()
        except Exception:
            city = address.split("-")[0].strip()

        utilities = response.css(".info-table > li:nth-child(6)::text").get()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("address", address)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("city", city)
        item_loader.add_value("utilities", utilities)
       
        yield item_loader.load_item()
