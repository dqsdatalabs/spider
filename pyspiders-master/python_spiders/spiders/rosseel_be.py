# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy.spiders import SitemapSpider
from w3lib.html import remove_tags
from python_spiders.items import ListingItem
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(SitemapSpider):
    name = "rosseel_be"
    start_urls = ["https://www.rosseel.be/te-huur"]
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    
    def start_requests(self):
        yield Request(self.start_urls[0], callback=self.parse)
    
    def parse(self, response):
        for item in response.xpath("//a[@class='biglink']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(f_url, callback=self.populate_item)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        remove = "".join(response.xpath("//div[@class='container']/text()[contains(.,'Pand')]").extract())
        if remove:
            return

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        bathroom_count = response.xpath("//span[contains(.,'Badkamer')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div[@id='descriptionbox']//p/text()").getall())
        if desc:
            available_date = 'pass'
            if len(desc.split('beschikbaar per')) > 1:
                if len(desc.split('beschikbaar per')[1].strip().split(' ')[0].split('/')) > 2:
                    available_date = desc.split('beschikbaar per')[1].strip().split(' ')[0]
            elif len(desc.split('beschikbaar vanaf')) > 1:
                if len(desc.split('beschikbaar vanaf')[1].strip().split(' ')[0].split('/')) > 2:
                    available_date = desc.split('beschikbaar vanaf')[1].strip().split(' ')[0]
            if available_date != 'pass':
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)
        
        energy_label = response.xpath("//span[contains(.,'EPC')]/following-sibling::span/text()").get()
        if energy_label:
            energy_label = float(energy_label.strip().split(' ')[0].strip())
            if energy_label >= 92:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 81 and energy_label <= 91:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 69 and energy_label <= 80:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 55 and energy_label <= 68:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 39 and energy_label <= 54:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 21 and energy_label <= 38:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 1 and energy_label <= 20:
                item_loader.add_value("energy_label", 'G')

        address = response.xpath("//div[@class='panel-body']//a[@class='overview']/text()").extract_first()

        item_loader.add_value("external_source", "Rosseel_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("description", "//div[@id='descriptionbox']/p/text()")
        
        price = "".join(response.xpath("//div[@class='overlay_bar']//li[@class='price']//text()").extract())
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
            
        ref = response.xpath("//span[@class='makelaartitle']/text()").get()
        if ref:
            ref = ref.split(":")[1]
            item_loader.add_value("external_id", ref.strip())

        square = response.xpath(
            "(//div[@class='col-md-12 col-lg-6 pandinfo'])[1]/ul/li[./span[.='Bebouwde opp.' or .='Bewoonbare opp.']]/span[@class='value']/text()"
        ).get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        room_count = response.xpath(
            "//div[@class='col-md-12 col-lg-6 pandinfo']/ul/li[./span[.='Badkamers' or .='Slaapkamers']]/span[@class='value']/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        floor = response.xpath(
            "//div[@class='col-md-12 col-lg-6 pandinfo'][1]/ul/li[./span[.='Verdieping']]/span[@class='value']/text()"
        ).get()
        if floor:
            item_loader.add_value("floor", floor)
        else:
            floor = response.xpath(
                "//div[@class='col-md-12 col-lg-6 pandinfo'][2]/ul/li[./span[.='Verdieping']]/span[@class='value']"
            ).get()
            item_loader.add_value("floor", floor)

        terrace = response.xpath(
            "(//div[@class='col-md-12 col-lg-6 pandinfo'])[3]/ul/li[./span[.='Terras']]/span[@class='value']/text()"
        ).get()
        if terrace:
            if terrace != "Nee":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        terrace = response.xpath(
            "(//div[@class='col-md-12 col-lg-6 pandinfo'])[3]/ul/li[./span[.='Parking']]/span[@class='value']/text()"
        ).get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        terrace = response.xpath(
            "//div[@class='col-md-12 col-lg-6 pandinfo']/ul/li[./span[.='Lift']]/span[@class='value']/text()"
        ).get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//section[@class='center slider']/div/img/@src"
            ).getall()
        ]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        latlang = response.xpath("//div[@class='pubmap']")
        if latlang:
            latitude = latlang.xpath("./@data-lat").get()
            longitude = latlang.xpath("./@data-long").get()
            if latitude and longitude:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

        item_loader.add_xpath(
            "landlord_name", "//div[@class='makelaarCard']//span[@class='naam']/text()"
        )
        item_loader.add_xpath(
            "landlord_phone",
            "//div[@class='makelaarCard']//span[@class='nummer']/text()",
        )

        json_data = response.xpath(
            "//script[@type='application/ld+json'][1]/text()"
        ).extract_first()
        data = json.loads(json_data)
        if "additionalType" in data.keys():
            prop_type = data["additionalType"]
            if "Appartement" in prop_type:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            elif "Woning" in prop_type:
                property_type = "house"
                item_loader.add_value("property_type", property_type)
            elif "Studentenkamer" in prop_type:
                property_type = "student apartment"
                item_loader.add_value("property_type", property_type)
            
        if item_loader.get_collected_values("property_type"):   
            yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1].strip()

        if get == "zip":
            return zip_code
        else:
            return city
