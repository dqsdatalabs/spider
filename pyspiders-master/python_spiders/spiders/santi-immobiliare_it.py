# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class SantiImmobiliareSpider(Spider):
    name = 'santi_immobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.santi-immobiliare.it"]
    start_urls = ["https://www.santi-immobiliare.it/ita/immobili?order_by=&page=&rental=1&company_id=&seo=&luxury=&categories_id=&property_type_id=&property_subtype_id=&rental=1&typologies_multi%5B%5D=1002&typologies_multi%5B%5D=1003&typologies_multi%5B%5D=1004&typologies_multi%5B%5D=1016&typologies_multi%5B%5D=1006&typologies_multi%5B%5D=1010&typologies_multi%5B%5D=1001&city_id=&code=&size_min=&size_max=&price_min=&price_max="]
    count = 0

    def parse(self, response):
        for page in response.css("a.foto::attr(href)").getall():
            yield Request(response.urljoin(page), callback=self.populate_item)

    def populate_item(self, response):
        data = {}

        title = response.css("h1.title span.type::text").get()
        if (("negozi" in title.lower()) or ("ufficio" in title.lower()) or ("box" in title.lower()) or ("auto" in title.lower())):
            return

        property_type = "Apartment"
        rent = response.css('span[title="Prezzo"] + b::text').get()
        energy_label = response.css('span[title="Classe Energ."] + b::text').get()
        square_meters = response.css('span[title="MQ"] + b::text').get()
        room_count = response.css('span[title="Locali"] + b::text').get()
        address = response.css("span.location::text").get()
        city = address.split(", ")[0]
        images = response.css("img.sl::attr(src)").getall()
        description = response.css(".description::text").get()
        external_id = response.css(".code::text").get()
        
        bathroom_count = response.css("span[title='Bagni']::text").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()

        data["title"] = title
        data["rent"] = rent
        data["property_type"] = property_type
        data["energy_label"] = energy_label
        data["square_meters"] = square_meters
        data["room_count"] = room_count
        data["address"] = address
        data["city"] = city
        data["images"] = images
        data["description"] = description
        data["external_id"] = external_id
        data["external_link"] = response.url
        data["bathroom_count"] = bathroom_count

        contact_page = response.css(".navigation-mobile > li:nth-child(6) > a:nth-child(1)::attr(href)").get()
        
        yield Request(response.urljoin(contact_page), callback=self.get_contacts, meta={"data": data}, dont_filter = True)


    def get_contacts(self, response):
        item_loader = ListingLoader(response=response)
        
        data = response.meta.get("data")

        landlord_name = "SANTI IMMOBILIARE"
        landlord_phone = response.css("div.left-contact:nth-child(2) > div:nth-child(2) > a:nth-child(2)::text").get().split(":")[1]
        landlord_email = response.css("div.left-contact:nth-child(2) > div:nth-child(2) > a:nth-child(3)::text").get()

        item_loader.add_value("external_link", data["external_link"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", data["property_type"])
        item_loader.add_value("address", data["address"])
        item_loader.add_value("rent_string", data["rent"])
        item_loader.add_value("title", data["title"])
        item_loader.add_value("energy_label", data["energy_label"])
        item_loader.add_value("square_meters", data["square_meters"])
        item_loader.add_value("room_count", data["room_count"])
        item_loader.add_value("city", data["city"])
        item_loader.add_value("images", data["images"])
        item_loader.add_value("description", data["description"])
        item_loader.add_value("external_id", data["external_id"])
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("bathroom_count", data["bathroom_count"])
                   
        yield item_loader.load_item()
        