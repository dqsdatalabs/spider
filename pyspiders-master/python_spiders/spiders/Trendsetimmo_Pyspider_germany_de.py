# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_unicode_char, extract_number_only

class TrendsetimmoPyspiderGermanyDeSpider(scrapy.Spider):
    name = "Trendsetimmo_Pyspider_germany_de"
    start_urls = [
        'https://trend-set-immo.de/marktplatz/mieten/wohnung-mieten.html', 'https://trend-set-immo.de/marktplatz/mieten/haus-mieten.html']
    allowed_domains = ["trend-set-immo.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        pages_count = len(response.css(
            "ul.pagination.pagination_right li")) - 1
        if pages_count == 0:
            pages_count = 1

        url_base = response.url[:response.url.find(".html")] + '/'
        for page in range(0, pages_count):
            url = url_base + str(page + 1) + '.html'
            yield scrapy.Request(url, callback=self.parse_pages)

    # 3. SCRAPING level 3
    def parse_pages(self, response, **kwargs):
        urls = response.css(
            "div.default-view-tile div.row div.col-sm-12.col-md-6.col-lg-6 a::attr(href)").getall()
        urls = ['https://trend-set-immo.de' +
                url for url in urls if "javascript:setFavorite" not in url]
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 4. SCRAPING level 4
    def populate_item(self, response):
        images = response.css(
            "div.col-xs-12 div.bs3-color1 img::attr(src)").getall()
        images = ['https://trend-set-immo.de' +
                  img.replace("100x100", '900x0') for img in images]
        images = [img for img in images if img !=
                  "https://trend-set-immo.de/components/com_sesimmotool/includes/images/spacer.png"]
        
        address = response.css("div.col-xs-12.col-sm-6 div.bs3-color1 div span.field-sesimmotool.field-plz::text").get() + response.css(
            "div.col-xs-12.col-sm-6 div.bs3-color1 div span.field-sesimmotool.field-ort::text").get()
        address = remove_unicode_char(address)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, _ = extract_location_from_coordinates(
            longitude, latitude)
        property_type = response.css(
            "span.field-sesimmotool.field-objektart::text").get()
        if property_type == "wohnung":
            property_type = "apartment"
        else:
            property_type = "house"
        square_meters = response.css(
            "span.field-sesimmotool.field-wohnflaeche::text").get()
        square_meters, _ = square_meters.split(" ")
        square_meters = square_meters.split(",")[0]

        room_count = response.css(
            "span.field-sesimmotool.field-anzahl_zimmer::text").get()
        deposit  = response.css(
            "span.field-sesimmotool.field-kaution::text").get()
        if deposit:
            deposit = deposit.split(" ")[0]
            deposit= extract_number_only(deposit)
        room_count = room_count.split(",")[0]

        bathroom_count = response.css(
            "span.field-sesimmotool.field-anzahl_badezimmer::text").get()

        rent = response.css(
            "span.field-sesimmotool.field-nettokaltmiete::text").get()
        rent, currency = rent.split(" ")

        heating_cost = response.css(
            "span.field-sesimmotool.field-warmmiete::text").get()
        heating_cost, currency = heating_cost.split(" ")

        extra_cost = response.css(
            "span.field-sesimmotool.field-nebenkosten::text").get()
        extra_cost, currency = extra_cost.split(" ")

        rent = int(rent.split(",")[0])
        heating_cost = int(heating_cost.split(",")[0]) - rent
        rent = rent + int(extra_cost.split(",")[0])

        descriptions = response.css(
            "span.field-sesimmotool.field-texte_beschreibung::text").getall()
        description = ''
        description = [description + element for element in descriptions]
        title = response.css(
            "span.field-sesimmotool.field-texte_titel::text").get()

        landlord_number = response.css(
            "span.field-sesimmotool.field-kontakt_tel_durchwahl::text").get()
        landlord_email = response.css(
            "span.field-sesimmotool.field-kontakt_email_zentrale::text").get()

        landlord_name = response.css(
            "span.field-sesimmotool.field-kontakt_anrede::text").get() + response.css(
            "span.field-sesimmotool.field-kontakt_name::text").get()
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("currency", currency)  # String
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("heating_cost", heating_cost)  # Int
        item_loader.add_value("landlord_name", remove_unicode_char(landlord_name))  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
