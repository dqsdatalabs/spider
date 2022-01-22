# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
import scrapy
from scrapy.selector import Selector
from ..helper import remove_white_spaces, string_found
from ..loaders import ListingLoader

class SorencoSpider(scrapy.Spider):
    name = "sorenco"
    allowed_domains = ["sorenco.be"]
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    current_page = 1

    def start_requests(self):
        urls = [
            {'url': 'https://www.sorenco.be/aanbod/te-huur/woningenvilla/',
                'property_type': 'house', 'request_type': 'initial'},
            {'url': 'https://www.sorenco.be/aanbod/te-huur/appartementen/',
                'property_type': 'apartment', 'request_type': 'initial'}
            ]
        for url in urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'),
                                       'request_type': url.get('request_type')})

    def parse(self, response, **kwargs):
        sel = Selector(text=response.body)
        request_type = response.meta.get('request_type')
        if request_type != 'initial':
            response_json = json.loads(response.text)
            response_body = remove_white_spaces(
                response_json.get('html', None))
            if response_body:
                sel = Selector(text=response_body)
            else:
                print("No next page available, Stopping")
                return
        else:
            sel = response
        listings = sel.css("a.property-contents-link::attr(href)").extract()
        for property_item in listings:
            yield scrapy.Request(url=property_item,
                                 callback=self.get_details,
                                 meta = {'property_type': response.meta.get('property_type')})

        # Pagination logic
        self.current_page = self.current_page + + 1
        next_page_url = "https://www.sorenco.be/properties/more/{}/te-huur/appartementen/".format(
            self.current_page)
        yield scrapy.Request(next_page_url, callback=self.parse,
                             meta = {'property_type': response.meta.get('property_type'),
                                     'request_type': 'pagination'})

    def get_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])

        utilities = response.xpath("//dt[contains(.,'Kosten')]/following-sibling::dd/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities)))

        item_loader.add_value("title", response.url.split("/")[-3].strip().replace("-", " "))

        item_loader.add_value("external_source", "Sorenco_PySpider_belgium_nl")
        item_loader.add_value("external_link", response.url)
        item_loader.add_css("images", "a.lightGallery::attr(href)")
        address = ''.join(response.css("div.address::text").extract())
        address = address.split('-')[0]
        item_loader.add_value("address", address)
        city_zip = address.split(",")
        if len(city_zip) == 2:
            zipcode = city_zip[1].strip().split(" ")[0]
            city = city_zip[1].split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        else:
            if " " in city_zip[0].strip():
                item_loader.add_value("zipcode",city_zip[0].strip().split(" ")[0])
                item_loader.add_value("city",city_zip[0].strip().split(" ")[1])
            else:
                item_loader.add_value("city", city_zip[0].strip())
        
        item_loader.add_css("rent_string", "div.price::text")
        item_loader.add_css("room_count", "li.rooms::text")
        item_loader.add_css("bathroom_count", "li.bathrooms::text")
        item_loader.add_css("square_meters", "li.area::text")
        garage_string = ''.join(response.css("li.garages *::text").extract())
        if garage_string:
            item_loader.add_value("parking", True)
        # item_loader.add_css("title", "article#prop__desc strong:first-child ::text")
        item_loader.add_css("description", "article#prop__desc *::text")
        elevator_string = ''.join(response.css("dt:contains('Lift')+dd *::text").extract())
        if string_found(['nee'], elevator_string):
            item_loader.add_value("elevator", False)
        elif string_found(['ja'], elevator_string):
            item_loader.add_value("elevator", True)
        terrace_string = ''.join(response.css("dt:contains('Terrassen')+dd *::text").extract())
        if terrace_string:
            item_loader.add_value("terrace", True)
        item_loader.add_css("energy_label", "dt:contains('EPC:')+dd *::text")
        item_loader.add_css("landlord_name", "section#prop__contact h2::text")
        item_loader.add_value("landlord_phone", "+ 32 3 281 08 08")
        item_loader.add_value("landlord_email", "info@sorenco.be")
        item_loader.add_css("external_id", "input[name='property_id']::attr(value)")
        js_code = response.xpath(".//script[contains(., 'const lat')]//text()").extract_first()
        js_code = remove_white_spaces(js_code).split(";")
        for var in js_code:
            if 'lat' in var:
                item_loader.add_value("latitude", var.split('=')[-1])
            if 'lng' in var:
                item_loader.add_value("longitude", var.split('=')[-1])
        item_loader.add_css('floor', "dt:contains('Verdieping:')+dd *::text")
        yield item_loader.load_item()