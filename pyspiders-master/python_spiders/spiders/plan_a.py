# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from datetime import datetime
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address


def get_bg_img(text):
    return text.replace("background-image:", "").replace("url(", "").replace(")", "").replace(";", "").strip()

class Plan_a_PySpider_canada_fr(scrapy.Spider):
    name = "plan_a"
    start_urls = ['https://plan-a.ca/fr/recherche?type=residential']
    allowed_domains = ["plan-a.ca"]
    country = 'canada'
    locale = 'fr'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for url in response.css("a.building::attr('href')").extract():
            yield scrapy.Request(url, callback=self.extract_data)

    def extract_data(self, response):
        general_data = {}
        general_data['title'] = response.css(".hero-title::text").get()
        general_data['landlord_name'] = response.css(".site-header img::attr('alt')").get()
        general_data['landlord_email'] = response.xpath("//a[contains(@href, 'mailto')]/@href").get().replace("mailto:", "")
        general_data['landlord_phone'] = response.xpath("//a[contains(@href, 'tel')]/@href").get().replace("tel:", "")
        general_data['images'] = response.css(".carousel-cell img::attr('data-flickity-lazyload')").extract()
        general_data['address'] = response.css(".hero-subtitle::text").get()
        general_data['city'], general_data['zipcode'] = general_data['address'].split(", ")[-2:]
        general_data['latitude'] = response.css("a::attr('data-search-map-lat')").get()
        general_data['longitude'] = response.css("a::attr('data-search-map-lng')").get()
        if not general_data['latitude'] or not general_data['longitude']:
            general_data['longitude'], general_data['latitude'] = extract_location_from_address(general_data['address'])

        amenities = []
        for section in response.css(".content-informations .content-information p::text").extract():
            amenities.append(section)
        for section in response.css(".slider-tab::text").extract():
            amenities.append(section)
        for section in response.css(".content"):
            if "Les appartements" in section.css(".content-title::text").get(""):
                txt = section.css(".content-title + .content-text")
                description_items = [x.strip() for x in txt.css("p::text").extract() if x.strip()] + txt.css("li::text").extract()
                general_data['description'] = "\r\n".join(description_items)
                tab = section.css(".content-slider-wrapper")[0]
                amenities.extend(tab.css("li::text").extract() + description_items)
                general_data['floor_plan_images'] = list(map(get_bg_img, tab.css(".visual::attr('style')").extract()))

        for line in amenities:
            if "balcon" in line or "balcony" in line:
                general_data['balcony'] = True
            elif "lavage" in line or "laveuse" in line:
                general_data['washing_machine'] = True
            elif "Lave-vaisselle" in line:
                general_data['dishwasher'] = True
            elif "Ascenseurs" in line:
                general_data['elevator'] = True
            elif "stationnement" in line:
                general_data['parking'] = True
            elif "La piscine" in line:
                general_data['swimming_pool'] = True
            elif "La terrasse" in line:
                general_data['terrace'] = True

        rooms = list(map(lambda x: int(x.split(" ")[0]), response.css(".tab-text::text").extract()))
        for i, table in enumerate(response.css(".unit-grid")):
            room_count = rooms[i]
            bathroom_count = 1 if room_count <= 2 else 2
            for item in table.css(".unit-row"):
                specific_data = {}
                specific_data['external_id'] = str(item.css(".unit-col-code::attr('data-sort-value')").get())
                specific_data['floor'] = item.css(".unit-col-floor::attr('data-sort-value')").get()
                specific_data['square_meters'] = item.css(".unit-col-area::attr('data-sort-value')").get()
                specific_data['available_date'] = datetime.fromtimestamp(int(item.css(".unit-col-date::attr('data-sort-value')").get())).strftime("%Y-%m-%d")
                specific_data['rent'] = item.css(".unit-col-rent::attr('data-sort-value')").get()

                item_loader = ListingLoader(response=response)
                self.populate_item(
                    item_loader,
                    {
                        **general_data, **specific_data, 'room_count': room_count, 'bathroom_count': bathroom_count,
                        'external_link': response.url + "#" + specific_data['external_id'],
                    },
                )
                yield item_loader.load_item()

    def populate_item(self, item_loader, data):
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", data.get("external_link"))
        item_loader.add_value("external_id", data.get("external_id"))
        item_loader.add_value("title", data.get("title"))

        item_loader.add_value("city", data.get("city"))
        item_loader.add_value("zipcode", data.get("zipcode"))
        item_loader.add_value("address", data.get("address"))
        item_loader.add_value("latitude", data.get("latitude"))
        item_loader.add_value("longitude", data.get("longitude"))
        item_loader.add_value("floor", data.get("floor"))

        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", data.get("square_meters"))
        item_loader.add_value("room_count", data.get("room_count"))
        item_loader.add_value("bathroom_count", data.get("bathroom_count"))
        item_loader.add_value("available_date", data.get("available_date"))

        item_loader.add_value("terrace", data.get("terrace"))
        item_loader.add_value("swimming_pool", data.get("swimming_pool"))
        item_loader.add_value("washing_machine", data.get("washing_machine"))
        item_loader.add_value("dishwasher", data.get("dishwasher"))
        item_loader.add_value("balcony", data.get("balcony"))
        item_loader.add_value("parking", data.get("parking"))
        item_loader.add_value("elevator", data.get("elevator"))

        item_loader.add_value("rent", data.get("rent"))
        item_loader.add_value("currency", "CAD")

        item_loader.add_value("landlord_name", data.get("landlord_name"))
        item_loader.add_value("landlord_phone", data.get("landlord_phone"))
        item_loader.add_value("landlord_email", data.get("landlord_email"))

        item_loader.add_value("floor_plan_images", data.get("floor_plan_images"))
        item_loader.add_value("images", data.get("images"))
        item_loader.add_value("description", data.get("description"))

        self.position += 1
