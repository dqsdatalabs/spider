# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from datetime import datetime
import math


class MySpider(Spider):
    name = "woonbureau_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.woonbureau.be/huren/?_sfm_prijs=0+100000&_sfm_slaapkamers=0+20&_sfm_type=Woning-%2C-", "property_type": "house"},
            {"url": "http://www.woonbureau.eu/te-huur?searchon=list&sorts=Dwelling&transactiontype=Rent", "property_type": "house"},
            {"url": "https://www.woonbureau.be/huren/?_sfm_prijs=0+100000&_sfm_slaapkamers=0+20&_sfm_type=Appartement-%2C-", "property_type": "apartment"},
            {"url": "http://www.woonbureau.eu/te-huur?searchon=list&sorts=Flat&transactiontype=Rent", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        if "https://www.woonbureau.be/huren/" in response.url:
            for item in response.xpath(
                "//div[@class='pandList']//div[contains(@class,'large-4 cell')]/div"
            ):
                follow_url = item.xpath("./a/@href").extract_first()
                square_meters = item.xpath(
                    ".//div[@class='descr']/span[contains(.,'Bewoonbare')]/text()"
                ).extract_first()
                if square_meters:
                    square_meters = math.ceil(
                        float(square_meters.split(":")[1].replace("m²", "").strip())
                    )
                    
                    yield Request(
                        follow_url,
                        callback=self.populate_item,
                        meta={"type": "niklaas", "square_meters": square_meters, 'property_type': response.meta.get('property_type')},
                    )
        else:
            for item in response.xpath(
                "//div[@data-view='showOnList']/a/@href"
            ).extract():
                follow_url = response.urljoin(item)
                yield Request(
                    follow_url, callback=self.populate_item, meta={"type": "lokeren", 'property_type': response.meta.get('property_type')}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Woonbureau_PySpider_" + self.country + "_" + self.locale)
        if "lokeren" in response.meta.get("type"):
            property_type =response.meta.get('property_type')
            prop_type = response.xpath("//tr[contains(.,'Type:')]/td[2]/text()[contains(.,'Studio')]").extract_first()
            if prop_type:
                property_type = "studio"
            item_loader.add_value("property_type",property_type)
            square_meters = response.xpath("//tr[contains(.,'Bewoonbare opp')]/td[2]/text()[not(contains(.,'0'))]").extract_first()
            room = response.xpath("//tr[td[.='Slaapkamers:']]/td[@class='kenmerk']/text()").extract_first()
            if room:
                item_loader.add_value("room_count", room)
            elif not room:
                if "studio" in property_type:
                    item_loader.add_value("room_count", "1")

            bathroom = response.xpath("//tr[td[.='Badkamers:']]/td[@class='kenmerk']/text()").extract_first()
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom)
            if square_meters:
                square_meters = square_meters.split("m²")[0].strip()
                square_meters = math.ceil(
                    float(square_meters)
                )
                item_loader.add_value("square_meters", str(square_meters))
            
            item_loader.add_value("external_link", response.url)
            
            title = "".join(
                response.xpath(
                    "//section[@class='container head']/h1/text()[1]"
                ).extract()
            )
            item_loader.add_value("title", re.sub("\s{2,}", " ", title).strip())
            item_loader.add_xpath("description", "//div[@id='description']/div/p")
            price = response.xpath(
                "//tr[td[.='Prijs:']]/td[@class='kenmerk']"
            ).extract_first()

            if price:
                item_loader.add_value(
                    "rent", price.split("€")[1].split("/")[0].replace("<", "")
                )
            item_loader.add_value("currency", "EUR")

            address = "".join(response.xpath("//section[@class='container head']/h1/text()[1]").getall()).strip()
            address = address.split(',')[-1].split('-')[-1].strip()

            elevator = response.xpath("//tr[td[.='Lift:']]/td[2]/text()").extract_first()
            if elevator:
                if elevator.strip() == "Ja":
                    item_loader.add_value("elevator", True)
                else:
                    item_loader.add_value("elevator", False)
            
            #item_loader.add_value("zipcode", split_address(address, "zip"))
            city = split_address(address, "city")
            city = re.sub('\s{2,}', ' ', city)
            item_loader.add_value("city", city)

            address_value = " ".join(response.xpath("//table//tr[contains(.,'Adres')]/td[2]/text()").extract())
            if address_value:
                item_loader.add_value("address", address_value)
            
            utilities = response.xpath("//table//tr[contains(.,'Totale kosten')]/td[2]/text()").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities.replace("€",""))

            item_loader.add_xpath(
                "external_id", "//tr[td[.='Referentie:']]/td[@class='kenmerk']"
            )

            zipcode = response.xpath("normalize-space(//section[@class='container head']/h1/text()[1])").get()
            if zipcode:
                zipcode_list = zipcode.split(" ")
                for i in zipcode_list:
                    if i.strip().isdigit() and len(i.strip()) > 2:
                        item_loader.add_value("zipcode", i.strip())
                        break
            
            

            date = response.xpath(
                "//tr[td[.='Beschikbaar vanaf:']]/td[@class='kenmerk']/text()[contains(.,'/')]"
            ).extract_first()
            if date:
                item_loader.add_value(
                    "available_date",
                    datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d"),
                )

            item_loader.add_xpath(
                "floor", "//tr[td[.='Op verdieping:']]/td[@class='kenmerk']/text()"
            )
            images = [
                response.urljoin(x)
                for x in response.xpath(
                    "//div[@class='owl-carousel']//a/@href"
                ).extract()
            ]
            item_loader.add_value("images", images)
            terrace = response.xpath(
                "//tr[td[.='Terras:']]/td[.='Ja']/text()"
            ).extract_first()
            if terrace:
                if terrace == "Ja":
                    item_loader.add_value("terrace", True)
                else:
                    item_loader.add_value("terrace", False)
            parking = response.xpath(
                "//tr[contains(.,'Parking') or contains(.,'Garage')]/td[2]//text()"
            ).get()
            if parking:
                if "nee" in parking.lower():
                    item_loader.add_value("parking", False)
                else:
                    item_loader.add_value("parking", True)

            item_loader.add_value("landlord_phone", "09 348 37 19")
            item_loader.add_value("landlord_email", "lokeren@woonbureau.be")
            item_loader.add_value("landlord_name", "NV Woonbureau")

            lat_long = response.xpath(
                "//script[@type='application/ld+json' and contains(.,'latitude')][1]/text()"
            ).extract_first()
            if lat_long:
                data = json.loads(lat_long)
                lat = data["geo"]["latitude"]
                log = data["geo"]["longitude"]

                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", log)

            pet = response.xpath(
                "//tr[contains(.,'Huisdieren toegelaten')]/td[2]/text()"
            ).extract_first()
            if pet:
                if "Neen" in pet:
                    item_loader.add_value("pets_allowed", False)
            yield item_loader.load_item()

        else:
            item_loader.add_value("external_link", response.url)
            title = "".join(response.xpath("//div[contains(@class,'title')]/h2/text()").extract())
            if "gemeubelde " in title:
                item_loader.add_value("furnished", True)
            if "autostaanplaats" in title:
                item_loader.add_value("parking", True)

            studio = response.xpath("//div[contains(@class,'title')]/h2/text()[contains(.,'studio')]").extract()
            if studio:
                item_loader.add_value("property_type", "studio")
            else:
                item_loader.add_value("property_type", response.meta.get('property_type'))

            item_loader.add_value("title", re.sub("\s{2,}", " ", title).lstrip("- "))
            item_loader.add_xpath("latitude", "//div[@class='acf-map']//@data-lat")
            item_loader.add_xpath("longitude", "//div[@class='acf-map']//@data-lng")
            item_loader.add_xpath("description", "//div/div[@class='large-8 cell']/p")
            price = response.xpath(
                "//div/div[@class='large-6 one cell']/p[contains(.,'Huurprijs:')]/span[contains(., '€')]"
            ).extract_first()
            if price:
                item_loader.add_value("rent", price.split("€")[1])
            item_loader.add_value("currency", "EUR")
            
            room = response.xpath("//div/div[contains(@class,'cell')]/p[contains(.,'Slaapkamers:')]/span/text()").extract_first()
            item_loader.add_value("room_count",room)
            bathroom = response.xpath("//div/div[contains(@class,'cell')]/p[contains(.,'Badkamers')]/span/text()").extract_first()
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom)
            square_meters = response.meta.get("square_meters")
            item_loader.add_value("square_meters",str(square_meters))

            ref = "".join(
                response.xpath(
                    "//div/div[@class='large-8 cell title']/h2/span[@class='pandID']/text()"
                ).extract()
            )
            item_loader.add_value("external_id", ref.strip())

            

            date = response.xpath(
                "//div/div[@class='large-6 one cell']/p[contains(.,'Beschikbaarheid:')]/span/text()[contains(.,'-')]"
            ).extract_first()
            if date:
                item_loader.add_value(
                    "available_date",
                    datetime.strptime(date, "%d-%m-%Y").strftime("%Y-%m-%d"),
                )

            item_loader.add_xpath(
                "floor",
                "//div/div[contains(@class,'cell')]/p[contains(.,'Verdieping:')]/span/text()",
            )

            terrace = response.xpath(
                "//div/div[contains(@class,'cell')]/p[contains(.,'Terras:')]/span/text()[contains(., 'Ja')]"
            ).get()
            if terrace:
                if terrace == "Ja":
                    item_loader.add_value("terrace", True)
                else:
                    item_loader.add_value("terrace", False)
            terrace = response.xpath(
                "//div/div[contains(@class,'cell')]/p[contains(.,'Lift')]/span/text()"
            ).get()
            if terrace:
                if "Ja" in terrace:
                    item_loader.add_value("elevator", True)
                else:
                    item_loader.add_value("elevator", False)

            images = [
                response.urljoin(x)
                for x in response.xpath("//ul/li/a/img/@src").extract()
            ]
            if images:
                item_loader.add_value("images", images)
            terrace = response.xpath(
                "//div/div[contains(@class,'cell')]/p[contains(.,'Garage:')]/span/text()[contains(., 'Ja')]"
            ).get()
            if terrace:
                if terrace == "Ja":
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)

            item_loader.add_value("landlord_phone", "03 777 36 77")
            item_loader.add_value("landlord_email", "info@woonbureau.be")
            item_loader.add_value("landlord_name", "Woonbureau Sint-Niklaas")

            address = response.xpath(
                "//div[contains(@class,'title')]/h1/text()"
            ).extract_first()
            if address:
                item_loader.add_value("address", address.replace("\t", "").strip())
                city = address.split(",")[1].strip()
                city = re.sub('\s{2,}', ' ', city)
                item_loader.add_value("city", city)
           
            utilities = response.xpath("//div/div[contains(@class,'cell')]/p[contains(.,'Gemeenschappelijke kosten')]/span/text()[.!='n.v.t']").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities.replace("€",""))
            energy_label = response.xpath("//div/div[contains(@class,'cell')]/p[contains(.,'EPC')]/span/text()").extract_first()
            if energy_label:
                energy_label = energy_label.split("KW")[0].strip()
                item_loader.add_value("energy_label", energy_label_calculate(int(float(energy_label.replace(",",".")))))
            yield item_loader.load_item()


def split_address(address, get):
    # temp = address.split(" ")[0]
    zip_code = "".join(filter(lambda i: i.isdigit(), address))
    city = address.split(" ")[-1]

    if get == "zip":
        return zip_code
    else:
        return city


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
