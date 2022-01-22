# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re

class MySpider(Spider):
    name = 'alpeshabitat_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Alpeshabitat_PySpider_france'
    def start_requests(self):
        headers = {
            'authority': 'alpeshabitat.fr',
            'x-requested-with': 'XMLHttpRequest',
            'accept': '*/*',
            'referer': '',
            'accept-language': 'tr,en;q=0.9',
            'Cookie': 'PHPSESSID=sug3jvrnm0pqci16tsn1m7it1p'
        }
        start_urls = [
            {
                "url" : "https://alpeshabitat.fr/json-search?budget-min=0&autre-bien=appartement&type=L&code_type=1100&page=1",
                "referer" : "https://alpeshabitat.fr/location-maison?budget-min=0&autre-bien=appartement",
                "property_type" : "apartment",
            },
            {
                "url" : "https://alpeshabitat.fr/json-search?budget-min=0&autre-bien=maison&type=L&code_type=1200&page=1",
                "referer" : "https://alpeshabitat.fr/location-maison?budget-min=0&autre-bien=maison",
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            headers["referer"] = item["referer"]
            yield Request(item["url"], headers=headers, callback=self.parse, meta={'property_type': item["property_type"], 'referer': item["referer"]})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        main_data = json.loads(response.body)
        data = json.loads(main_data["json"])
        for item in data:
            follow_url = response.meta["referer"].split('?')[0] + "/" + item["reference"]
            seen = True
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        if page == 2 or seen:
            headers = {
                'authority': 'alpeshabitat.fr',
                'x-requested-with': 'XMLHttpRequest',
                'accept': '*/*',
                'referer': '',
                'accept-language': 'tr,en;q=0.9',
                'Cookie': 'PHPSESSID=sug3jvrnm0pqci16tsn1m7it1p'
            }
            headers["referer"] = response.meta["referer"]
            follow_url = response.url.replace("&page=" + str(page - 1), "&page=" + str(page))
            yield Request(follow_url, headers=headers, callback=self.parse, meta={"property_type": response.meta["property_type"], 'referer': response.meta["referer"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Alpeshabitat_PySpider_france")
        
        title = response.xpath("//div[@class='col s12']/h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//span[@class='colorTitle']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        rent = "".join(response.xpath("//span[@class='price']/text()").getall())
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent", int(float(rent)))
        
        item_loader.add_value("currency","EUR")
        
        square_meters = response.xpath("//p/i[contains(.,'border')]/following-sibling::text()[not(contains(.,'bon état'))]").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//p/i[contains(.,'hotel')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        else:
            room_count = response.xpath("//p/i[contains(.,'photo')]/following-sibling::text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])
                
        bathroom_count = response.xpath("//p/i[contains(.,'hot_tub')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        utilities = response.xpath("//p[contains(.,'Charge')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        energy_label = response.xpath("//div[contains(@class,'dpe perf')]//div[contains(@class,'active')]/strong/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        desc = "".join(response.xpath("//h2[contains(.,'Ma location')]/../p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())
        
        images = [x for x in response.xpath("//div[@id='slider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//div/@data-lat").get()
        longitude = response.xpath("//div/@data-lng").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Alpes Isère Habitat")
        item_loader.add_value("landlord_phone", "")
        item_loader.add_value("landlord_email", "")

        yield item_loader.load_item()