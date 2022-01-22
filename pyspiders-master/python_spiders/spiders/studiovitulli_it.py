# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from scrapy import FormRequest
from ..loaders import ListingLoader
import json


class StudiovitulliItSpider(scrapy.Spider):
    name = 'studiovitulli_it'
    allowed_domains = ['studiovitulli.it']
    start_urls = [
        'https://www.studiovitulli.it/advanced-search/?keyword=&location=&area=&status=locazione&type=&label=residenziali&property_id=&bedrooms=&bathrooms=&min-area=&max-area=&classe-energetica1536312247f5b9243b7ad6fe=&stato-immobile1538558971f5bb48bfb336e1=&min-price=&max-price=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#content-area > div.property-listing.grid-view.grid-view-3-col > div>div"):
            yield Request(appartment.css("a.hover-effect").attrib['href'],
                          callback=self.page_follower,
                          )
        try:
            next_page = response.css('a[rel="Next"]').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def page_follower(self, response):
        appartment_url = response.css('link[rel="shortlink"]').attrib['href']
        formdata = {
            "action": "houzez_get_single_property",
            "prop_id": "{}".format(appartment_url.split("=")[-1].strip()),
            "security": response.css("#securityHouzezMap::attr(value)").get()
        }

        yield FormRequest(url="https://www.studiovitulli.it/wp-admin/admin-ajax.php",
                          callback=self.populate_item,
                          formdata=formdata,
                          meta={
                              "external_link": response.url,
                              "porperty": response
                          })

    # 2. SCRAPING level 2
    def populate_item(self, response):
        parsed_response = json.loads(response.body)
        item_loader = ListingLoader(response=response)
        appartment = response.meta["porperty"]

        title = appartment.css('h1::text').extract()[0].strip()

        rent = appartment.css(
            'span.item-price::text').get().split("€")[0].strip()

        if ',' in rent:
            rent = rent.split(',')[0]

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0]+rent_array[1]
        rent = int(rent)

        description = appartment.css("#description>p::text").get()

        address = appartment.css(
            'li.detail-area::text').get().strip()

        city = appartment.css(
            "#description > div.row.row-eq-height > div:nth-child(2) > ul > li.detail-city::text").get().strip()

        images = appartment.css('div.item>img::attr(src)').extract()

        features = appartment.css("div.alert.alert-info>ul.list-three-col>li")

        external_id = None
        elevator = None
        space = None
        rooms = None
        floor = None
        bathrooms = None
        furnished = None
        balcony = None
        parking = None
        for item in features:
            if "Rif. annuncio:" in item.css('strong::text').get():
                external_id = item.css("li::text").get().strip()
            elif "Superficie proprietà:" in item.css('strong::text').get():
                space = item.css("li::text").get().split("m")[0].split()
            elif "Vani:" in item.css('strong::text').get():
                rooms = item.css("li::text").get().strip()
            elif "Bagni:" in item.css('strong::text').get():
                bathrooms = item.css("li::text").get().strip()
            elif "Piano:" in item.css('strong::text').get():
                floor = item.css("li::text").get().strip()
            elif "Arredamento:" in item.css('strong::text').get() and item.css("li::text").get():
                furnished = item.css("li::text").get().strip()
                if "Completo" in furnished:
                    furnished = True
                else:
                    furnished = False
            elif "N. Balconi:" in item.css('strong::text').get():
                balcony = item.css("li::text").get().strip()
                if int(balcony) >= 1:
                    balcony = True
                else:
                    balcony = False
            elif "N. Posti auto:" in item.css('strong::text').get():
                parking = item.css("li::text").get().strip()
                if int(parking) < 1:
                    parking = False
                else:
                    parking = True
            elif "N. Ascensori:" in item.css('strong::text').get():
                elevator = item.css("li::text").get().strip()
                if int(elevator) >= 1:
                    elevator = True
                else:
                    elevator = False

        if len(images) < 3:
            return

        # # MetaData
        item_loader.add_value("external_link", response.meta["external_link"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", space)
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("floor", floor)
        item_loader.add_value("city", city)

        item_loader.add_value("latitude", parsed_response["props"][0]["lat"])
        item_loader.add_value(
            "longitude", parsed_response["props"][0]["lng"].split())

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        # House Rules
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)

        # LandLord Details
        item_loader.add_value("landlord_phone", "0805562599")
        item_loader.add_value("landlord_email", "info@studiovitulli.it")
        item_loader.add_value("landlord_name", "Studio Vitulli")

        yield item_loader.load_item()
