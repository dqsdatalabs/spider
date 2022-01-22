# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class RockAgentSpider(Spider):
    name = 'rockagent_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.rockagent.it"]
    start_urls = ["https://www.rockagent.it/affitto-case"]

    def parse(self, response):
        for url in response.css("div.pages a.page::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_page)

    def populate_page(self, response):
        for page in response.css("div.estate-list-item div.media div.media-right div.media-heading div.row div.col-title a::attr(href)").getall():
            yield Request(response.urljoin(page), callback=self.populate_item)


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css("div.section__heading div.row div.col-title h1::text").get()
        if (("Commerciale" in title) or ("Ufficio" in title) or ("Magazzino" in title) or("Negozio") in title):
            return

        property_type = "Apartment"
        
        furnished = response.css("div.section__details:nth-child(3) > ul:nth-child(2) > li:nth-child(7) > span:nth-child(2)::text").get()
        if "Arredato" not in furnished:
            furnished = response.css("div.section__details:nth-child(3) > ul:nth-child(2) > li:nth-child(8) > span:nth-child(2)::text").get()
        
        if "Arredato" not in furnished:
            furnished = response.css("div.section__details:nth-child(3) > ul:nth-child(2) > li:nth-child(9) > span:nth-child(2)::text").get()
        
        if "Arredato" not in furnished:
            furnished = response.css("div.section__details:nth-child(3) > ul:nth-child(2) > li:nth-child(6) > span:nth-child(2)::text").get()

        if "Non" in furnished:
            furnished = False
        else:
            furnished = True

        deposit = response.css("div.section__details:nth-child(6) > ul:nth-child(2) > li:nth-child(5) > span:nth-child(2)::text").get()
        if not deposit:
            deposit = response.css("div.section__details:nth-child(5) > ul:nth-child(2) > li:nth-child(5) > span:nth-child(2)::text").get()

        utilities = response.css("div.section__details:nth-child(6) > ul:nth-child(2) > li:nth-child(3) > span:nth-child(2)::text").get()
        if not utilities:
            utilities = response.css("div.section__details:nth-child(5) > ul:nth-child(2) > li:nth-child(3) > span:nth-child(2)::text").get()
        
        utilities = utilities.split("/")[0]

        description = response.css(".section__content > p:nth-child(2)::text").get().strip()

        longitude = response.css("div#estateMap::attr(data-longitude)").get()
        latitude = response.css("div#estateMap::attr(data-latitude)").get()

        landlord_email = response.css("div.col-md-3:nth-child(4) > div:nth-child(1) > p:nth-child(2) > a:nth-child(8)::text").get().strip()
        landlord_phone = response.css("#estate-contact-form > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(2) > a:nth-child(3)::text").get().strip()
        landlord_name = response.css("#estate-contact-form > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(2) > span:nth-child(1)::text").get().strip()
        
        square_meters = response.css("div.section__details:nth-child(3) > ul:nth-child(2) > li:nth-child(3) > span:nth-child(2)::text").get()
        rent = response.css("div.col-price span.price::text").get()

        energy_label = response.css("div.indicator-energy::text").get()
        energy_label = energy_label.split("Classe energetica ")[1]

        address = title.split("affitto, ")[1]
        city = address.split(", ")[-1]

        images = response.css("ul#imageGallery li img::attr(src)").getall()

        room_count = response.css(".col-info > ul:nth-child(1) > li:nth-child(2) > b:nth-child(1)::text").get()
        bathroom_count = response.css(".col-info > ul:nth-child(1) > li:nth-child(3) > b:nth-child(1)::text").get()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("address", address)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("city", city)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("description", description)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("furnished", furnished)
       
        yield item_loader.load_item()


    