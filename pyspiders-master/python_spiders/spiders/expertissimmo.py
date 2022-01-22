# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from scrapy import Request, FormRequest
import json

from python_spiders.helper import string_found


class ExpertissimmoSpider(scrapy.Spider):
    name = "expertissimmo"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = "Expertissimmo_PySpider_belgium_nl"
    scale_separator = ','
    thousand_separator = '.'
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {
                "url": "https://expertissimmo.eu/fr/proxy/https://api.whise.eu/v1/estates/list/proxydata/EstateServiceGetEstateListRequest=%7B%22Filter%22:%7B%22CategoryIds%22:[],%22EstateIds%22:[null],%22Furnished%22:null,%22LanguageId%22:%22fr-BE%22,%22MaxRooms%22:%22%22,%22MinRooms%22:%22%22,%22PurposeIds%22:[2],%22PurposeStatusIds%22:[2,4,6],%22PriceRange%22:%7B%22Min%22:0,%22Max%22:100000000%7D,%22ShowDetails%22:false,%22ZipCodes%22:[]%7D,%22Page%22:%7B%22Offset%22:80,%22Limit%22:80%7D,%22Sort%22:[%7B%22Field%22:%22purposeStatusId%22,%22Ascending%22:true%7D,%7B%22Field%22:%22updateDateTime%22,%22Ascending%22:false%7D]%7D",
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          headers=self.headers,
                          )

    def parse(self, response):

        if response.body:
            data_json = json.loads(str((response.body).decode()))
            estates = data_json["estates"]
            base_url = "https://expertissimmo.eu/fr/nos-biens/a-louer/"

            for estate in estates:
                follow_url = base_url + f"{estate['id']}/2-{estate['city']}"
                area = estate["area"]
                address = estate["address"]
                city = estate["city"]
                zip = estate["zip"]
                price = estate["price"]
                id = estate["id"]

                yield Request(follow_url,callback=self.get_property_details,meta={'area': area,'address': address,'city': city,'zip': zip,'price': price,'id': id})

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type = response.xpath("//h1[@class='heading']/text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", "house")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.meta.get("id")
        if external_id:
            external_id = str(external_id)
            item_loader.add_value("external_id", external_id)

        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address)

        city = response.meta.get("city")
        if city:
            item_loader.add_value("city", city)

        zipcode = response.meta.get("zip")
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        description = response.xpath(
            "//div[@class='content']/text()").getall()
        if description:
            item_loader.add_value("description", description)

        energy_label = response.xpath(
            "//ul/li[contains(@class,'icon')]/img/@src").get()
        if energy_label and "icon_" in energy_label.lower():
            energy_label = "".join(
                energy_label.split("icon_")[1].split(".")[0])
            item_loader.add_value("energy_label", energy_label)

        bathroom_count = response.xpath(
            "//ul//li//p[contains(.,'bains')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0].strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//ul/li/p[contains(.,'chambres')]/text()").get()
        if room_count:
            room_count = room_count.split("chambres")[0].strip()
            item_loader.add_value("room_count", room_count)

        square_meters = response.meta.get("area")
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        rent = response.meta.get("price")
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='banner_slider']//a[@class='img_popup']//@href").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Expertissimmo")
        item_loader.add_value("landlord_phone", "02 736 67 92")
        item_loader.add_value("landlord_email", "info@expertissimmo.eu")

        yield item_loader.load_item()
