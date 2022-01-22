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
import dateparser
import re

class MySpider(Spider):
    name = "bvm_vastgoed"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source='Bvmvastgoed_PySpider_belgium_nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.bvm-vastgoed.be/nl/te-huur/appartementen",
                "property_type" : "apartment",
                "type" : 2
            },
            {
                "url" : "https://www.bvm-vastgoed.be/nl/te-huur/woningen",
                "property_type" : "house",
                "type" : 1
            },
            {
                "url" : "https://www.bvm-vastgoed.be/nl/te-huur/studio-kamer",
                "property_type" : "studio",
                "type" : 3
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'), "type": url.get("type")})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 12)
        url_type = response.meta.get("type")

        seen = False
        for item in response.xpath(
            "//div[@class='property-list-item']/a/@href"
        ).extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 12 or seen:
            url = f"https://www.bvm-vastgoed.be/nl/?option=com_properties&view=listAjax&count={page}&ptype={url_type}&status=1&goal=1"
            yield Request(url, callback=self.parse, meta={"page": page + 12, "type": url_type, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Bvmvastgoed_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        description = " ".join(response.xpath("//div[@class='content']//p/text()").getall())
        if description:
            item_loader.add_value("description", description)
        
        if "€ provisie" in description:
            utilities = description.split("€ provisie")[0].strip().split(" ")[-1]
            if utilities.isdigit(): item_loader.add_value("utilities", utilities)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        bathroom_count = response.xpath("//div[contains(text(),'Aantal badkamers')]/../div[@class='value']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        price = response.xpath(
            "//div[@class='content']/div[@class='field' and ./div[@class='name' and .='Prijs:']]/div[@class='value']/text()"
        ).extract_first()
        if price:
            item_loader.add_value("rent", price.split("€ ")[1].strip().replace(",", ""))
        item_loader.add_value("currency", "EUR")

        external_id = response.url.split('id=')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//div[@class='swiper-container gallery-top swiper-container-horizontal']/div/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        energy_label = response.xpath("//div[contains(text(),'Energie certificaat')]/..//div[contains(text(),'EPC:')]/../div[@class='value']/text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(' ')[0].replace(',', '.')
            if not 'In' in energy_label:
                energy_label = float(energy_label)
                if energy_label >= 92 and energy_label <= 100:
                    energy_label = 'A'
                elif energy_label >= 81 and energy_label <= 91:
                    energy_label = 'B'
                elif energy_label >= 69 and energy_label <= 80:
                    energy_label = 'C'
                elif energy_label >= 55 and energy_label <= 68:
                    energy_label = 'D'
                elif energy_label >= 39 and energy_label <= 54:
                    energy_label = 'E'
                elif energy_label >= 21 and energy_label <= 38:
                    energy_label = 'F'
                elif energy_label >= 1 and energy_label <= 20:
                    energy_label = 'G'
                if type(energy_label) == str:
                    item_loader.add_value("energy_label", energy_label)
        
        desc = " ".join(response.xpath("//div[@class='content']//p/text()").getall()).strip()
        if desc:
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)

        square = response.xpath(
            "//div[@class='content']/div[@class='field' and ./div[@class='name' and .='Oppervlakte Totaal:']]/div[@class='value']/text()"
        ).extract_first()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        room = response.xpath(
            "//div[@class='content']/div[@class='field' and ./div[@class='name' and .='Aantal slaapkamers:']]/div[@class='value']/text()"
        ).get()

        item_loader.add_value("room_count", room)

        available_date = response.xpath(
            "//div[@class='content']/div[@class='field' and ./div[@class='name' and .='Beschikbaarheid:']]/div[@class='value']/text()[not(contains(.,'Onmiddellijk') or contains(.,'Overeen te komen'))]"
        ).get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        terrace = response.xpath(
            "//div[@class='group']//div[@class='field' and ./div[@class='name' and .='Terras:']]/div[@class='value']"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

        parking = response.xpath(
            "//div[@class='content']//div[@class='field' and ./div[.='Garage:']]/div[@class='value']/text()"
        ).get()
        if parking:
            if "Ja" in parking or (parking!='0' and parking.isdigit()):
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
            
        terrace = "".join(
            response.xpath(
                "//div[@class='span4']//div[@class='content']/div[@class='field' and ./div[@class='name' and .='Lift:']]/div[@class='value' and .='Ja']/text()"
            ).extract()
        )
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("elevator", True)
            elif "Yes" in terrace:
                item_loader.add_value("elevator", True)
            elif "No" in terrace:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", False)

        address = " ".join(
            response.xpath(
                "//div[@class='content']/div[@class='field' and ./div[@class='name' and .='Adres:']]/div[@class='value']/text()"
            ).extract()
        )
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))
        else:
            address = response.xpath("//title/text()").get()
            if address:
                city = address.split("(")[0].strip().split(" ")[-1]
                zipcode = address.split("(")[1].split(")")[0].strip()
                item_loader.add_value("address", city)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)

        item_loader.add_xpath(
            "landlord_name",
            "//div[@class='teammember']//span[@class='teammember-name']/text()",
        )
        phone = response.xpath(
            "//div[@class='teammember']/div[@class='teammember-info']/a/@href[contains(.,'tel:')]"
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))

        mail = response.xpath(
            "//div[@class='teammember']/div[@class='teammember-info']/a/@href[contains(.,'mailto:')]"
        ).get()
        if mail:
            item_loader.add_value("landlord_email", mail.replace("mailto:", ""))

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='swiper-wrapper']/div/img/@src"
            ).extract()
        ]
        item_loader.add_value("images", images)

        lat_long = response.xpath(
            "//div[@id='PropertyRegion']//div[@id='detailswitch3']/iframe/@src"
        ).get()
        if lat_long:
            lat_long = lat_long.split("sll=")[1]
            lat = lat_long.split(",")[0]
            longt = lat_long.split(",")[1].split("&")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", longt)
        
        yield item_loader.load_item()


def split_address(address, get):
    temp = address.split(" ")[-2]
    zip_code = "".join(filter(lambda i: i.isdigit(), temp))
    city = address.split(" ")[-1]

    if get == "zip":
        return zip_code
    else:
        return city
