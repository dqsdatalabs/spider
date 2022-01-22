# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = "rencura_be"
    start_urls = ["https://rencura.be/api/estates/1"]  # LEVEL 1
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    
    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)

        for item in data["data"]:
            if "te-huur" in item["url"]:
                category = item["category"]
                if category == "249086":
                    property_type = "apartment"
                    yield Request(item["url"], callback=self.populate_item, meta={'property_type': property_type})
                elif category == "249085":
                    property_type = "house"
                    yield Request(item["url"], callback=self.populate_item, meta={'property_type': property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # https://rencura.be/aanbod?transactions=12771 -- listing url
        item_loader.add_value("external_source", "Rencura_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip().replace("\n",""))
            item_loader.add_value("title", title)
        property_type =  response.meta.get("property_type")
        prop_type = price = response.xpath("//h1/text()").extract_first()
        if prop_type:
            if "studio" in prop_type.lower():
                property_type = "studio"

        item_loader.add_value("property_type",property_type)
        item_loader.add_value("external_link", response.url)
        square = response.xpath("//div[@class='ProjectInfo-icon']/text()[contains(., 'm') and .!=' 0m']").extract_first()
        utilities = response.xpath(
            "//text()[contains(.,'gemeenschappelijk kosten') and (contains(.,'€') and not(contains(.,'{')))]"
        ).get()
        if utilities:
            item_loader.add_value("utilities", utilities)

        desc = "".join(response.xpath("//div[contains(@class,'Content')]/h1/following-sibling::div[not(@class)]//text()").extract())
        if not desc:
            desc = "".join(response.xpath("//h1[@class='ProjectDetail-title']/following-sibling::*/text()").extract())
            if desc:
                desc = desc.strip()
            if not desc:
                desc = "".join(response.xpath("//div[contains(@class,'Content')]/span//text()").extract())
                if not desc:
                    desc = "".join(response.xpath("//h1[@class='ProjectDetail-title']/following-sibling::p//text()").extract())
                    if not desc:
                        desc = "".join(response.xpath("//div[contains(@class,'Content')]/h1/following-sibling::text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            try:
                if "verdieping" in desc:
                    floor = desc.split("verdieping")[0].strip().split(" ")[-1].strip()
                    if floor:
                        item_loader.add_value("floor",floor)            
            except:
                pass
            try:
                if "euro maandelijkse gemeenschappelijke" in desc:
                    utilities = desc.split("euro maandelijkse gemeenschappelijke")[0].strip().split(" ")[-1].strip()
                    if utilities.isdigit():
                        item_loader.add_value("utilities",utilities)
                elif "gemeenschappelijke kosten" in desc.lower():
                    utilities = desc.lower().split("gemeenschappelijke kosten")[1].strip().split("eur")[0].strip().split(" ")[1]
                    if utilities and utilities.isdigit():
                        item_loader.add_value("utilities",utilities)
            except:
                pass
   

        price = response.xpath(
            "//div[@class='ProjectInfo-price']/text()[contains(., '€')]"
        ).extract_first()
        if price:
            # item_loader.add_value("rent", price.split("€")[1])
            # item_loader.add_value("currency", "EUR")
            item_loader.add_value("rent_string", price)
        ref = "".join(
            response.xpath("//span[@class='ProjectDetail-reference']//text()").extract()
        )
        if ref:
            if ":" in ref:
                ref = ref.strip().split(":")[1]
                item_loader.add_value("external_id", ref)

        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        available_date = response.xpath(
            "//dl[@class='Table-list Table-list--columns']/div[./dt[.='Beschikbaar vanaf']]/dd/text()[. != 'onmiddellijk' and . != 'vanaf akte' and . != 'bij oplevering' and . != 'in onderling overleg' and . != 'af te spreken met eigenaar' and . != 'mits inachtneming huurders']"
        ).get()
        if available_date:
            item_loader.add_value("available_date", available_date)        

        room = response.xpath(
            "//dl[@class='Table-list Table-list--columns']/div[./dt[text()='Slaapkamers']]/dd/text()"
        ).get()
        if room:
            item_loader.add_value("room_count", room)

        bathroom = response.xpath("//dl[@class='Table-list Table-list--columns']/div[./dt[text()='Badkamers']]/dd/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)

        terrace = response.xpath(
            "//dl[@class='Table-list Table-list--columns']/div[./dt[text()='Terras']]/dd/text()"
        ).get()
        if terrace:
            if "ja" in terrace.lower() or "Yes" in terrace:
                item_loader.add_value("terrace", True)
            elif "No" in terrace or "nee" in terrace.lower():
                item_loader.add_value("terrace", False)
            
        parking = response.xpath(
            "//dl[@class='Table-list Table-list--columns']/div[./dt[.='Garage'] or ./dt[.='Parking']]/dd/text()"
        ).get()
        if parking:
            if "No" in parking or "nee" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        furnished = response.xpath(
            "//dl[@class='Table-list Table-list--columns']/div[./dt[.='Gemeubeld']]/dd/text()"
        ).get()
        if furnished:
            if "ja" in furnished.lower() or "yes" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "no" in furnished.lower() or "nee" in furnished.lower():
                item_loader.add_value("furnished", False)

        elevator = response.xpath(
            "//dl[@class='Table-list']/div[./dt[.='Lift']]/dd/text()"
        ).get()
        if elevator:
            if "ja" in elevator.lower() or "Yes" in elevator:
                item_loader.add_value("elevator", True)
            elif "No" in elevator or "nee" in elevator.lower():
                item_loader.add_value("elevator", False)

        swimming_pool = response.xpath(
            "//dl[@class='Table-list Table-list--columns']/div[./dt[.='Zwembad']]/dd/text()"
        ).get()
        if swimming_pool:
            if "ja" in swimming_pool.lower() or "Yes" in swimming_pool:
                item_loader.add_value("swimming_pool", True)
            elif "No" in swimming_pool or "nee" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='Thumbnail-items']/div/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath(
            "energy_label",
            "//dl[@class='Table-list Table-list--columns']/div[./dt[.='EPC-label']]/dd/text()[ . != 'IN AANVRAAG']",
        )

        address ="".join( response.xpath(
            "//a[@class='AddressLine AddressLine--full u-marginBlg js-fakeAddressLine']//text()"
        ).extract())
        item_loader.add_value("address", address)
        
        city_zip = response.xpath("//script[contains(.,'location') and contains(.,'addressLocality')]/text()").get()
        if city_zip:
            item_loader.add_value("zipcode", city_zip.split('"postalCode": "')[1].split('"')[0].strip())
            item_loader.add_value("city", city_zip.split('"addressLocality": "')[1].split('"')[0].strip())
        item_loader.add_xpath("landlord_name", "//h2[@class='ProjectContact-name']//text()")
        item_loader.add_value("landlord_email", "info@rencura.be")

        item_loader.add_xpath(
            "landlord_phone", "//div[@class='ProjectContact-phone']/a//text()"
        )

        latlng = response.xpath("//script[contains(.,'window.markers')]/text()").get()
        if latlng:
            lat = latlng.split("lat:")[1].split(",")[0].strip()
            lng = latlng.split("lng:")[1].split(",")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        yield item_loader.load_item()

