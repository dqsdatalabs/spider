# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import math
import re

class MySpiderA(Spider):
    name = "walls_be"
    custom_settings = {
        "PROXY_ON": True,
    }
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = 'Walls_PySpider_belgium_nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.walls.be/wonen/te-huur?query={%22category%22:%222%22}"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'galcell')]"):
            follow_url = response.urljoin(item.xpath(".//@href").extract_first())
            zip_city = item.xpath(".//div[@class='item--city']/text()").extract_first()
            property_type = item.xpath(".//div[@class='item--info']/h2/text()").get()
            if (get_p_type_string(property_type)):
                yield Request(
                    follow_url, callback=self.populate_item, meta={"zip_city": zip_city, 'property_type': (get_p_type_string(property_type))}
                )
            seen = True

        if page == 2 or seen:
            url = f"https://www.walls.be/ajax/{page}"
            yield Request(url, callback=self.parse, meta={"page": page + 1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        zip_city = response.meta.get("zip_city")
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Walls_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='col-sm-6']/h2//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        desc = " ".join(response.xpath("//div[contains(@class,'article-wrapper')]/article/div[@class='row'][2]/div/text()[normalize-space()]").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value(
                "description", desc.strip()
            )
            # if "terras " in desc:
            #     item_loader.add_value("terrace", True)
            # if " lift" in desc:
            #     if "geen lift" in desc.lower():
            #         item_loader.add_value("elevator", False)
            #     else:
            #         item_loader.add_value("elevator", True)
            # if " vaatwasser" in desc:
            #     item_loader.add_value("dishwasher", True) 
            # if " balkonnetje" in desc:
            #     item_loader.add_value("balcony", True)
            # if " gemeubileerde" in desc:
            #     item_loader.add_value("furnished", True)
        elif not desc:
            desc = "".join(response.xpath("//div[contains(@class,'article-wrapper')][1]/article/div[@class='row'][2]/div[1]//text()").extract())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc)
                item_loader.add_value(
                    "description", desc.strip()
                )
        price = response.xpath(
            "//div[@class='detail-info']/div[@class='price']/div[@class='item--price'][contains(., '€')]"
        ).extract_first()
        if price:
        #     item_loader.add_value("rent", price.split("€")[1].split("/")[0])
        # item_loader.add_value("currency", "EUR")
            item_loader.add_value("rent_string", price.replace(" ",""))
        utilities= response.xpath("//tr[td[.='Lasten per maand']]/td[2]/text()[contains(.,'€')]").extract_first()
        if utilities :
            item_loader.add_value("utilities", utilities.split("€")[1].strip())
        deposit = response.xpath("//tr[td[.='Huurgarantie']]/td[2]//text()").extract_first()
        if deposit :
            if "maand" in deposit:
                deposit_value = deposit.split("maand")[0].strip()
                if deposit_value and price:
                    price = price.split("€")[1].split("/")[0].replace(" ","").replace(".","")
                    deposit = int(deposit_value)*int(float(price.replace(",",".")))
            item_loader.add_value("deposit", deposit)        
            
        external_id = response.xpath("normalize-space(//tr[td[.='Referentie nr.']]/td[2]/text())").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        square = response.xpath("//tr[td[.='Opp.']]/td[2]/text()").extract_first()

        room = response.xpath("//tr[td[.='Slaapkamers']]/td[2]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        bathroom_count  = response.xpath("//div[@class='detail-info']//div[contains(@class,'bathroom')]/text()").extract_first()
        if bathroom_count :
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        if square:
            square = square.split("ca.")[1].strip().split("m²")[0].strip()
            square = math.ceil(float(square))
            item_loader.add_value(
                "square_meters", str(square)
            )
            
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_xpath("latitude", "//div[@id='map-canvas']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@id='map-canvas']/@data-lng")

        floor = response.xpath(
            "normalize-space(//tr[td[.='Verdieping']]/td[2]/text())"
        ).extract_first()
        if floor:
            if "A" in floor:
                floor = floor.split("A")[0].strip()
            item_loader.add_value("floor", floor.replace("L", ""))

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='foto-accordion col-sm-12 estate-detail detail']/ul//a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        terrace = response.xpath("//tr[td[.='Terras'  or .='Terras opp.']]/td[2] | //div[@class='detail-info']//div[contains(@class,'terrace')]//text()[.='Ja']").get()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath(
            "//tr[td[.='Parkings (binnen)' or .='Garages']]/td[2]/text()"
        ).get()
        if terrace:
            item_loader.add_value("parking", True)

        terrace = response.xpath(
            "normalize-space(//tr[td[.='Lift']]/td[2]/text())"
        ).get()
        if terrace:
            if terrace == "Ja" or terrace == "Oui":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        landlord_name = response.xpath("//div[@class='realtor--info']/p[@class='pink']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Walls Vastgoedmakelaars")
      
        phone = response.xpath('substring-after(//div[@class="realtor--info"]/p/a[contains(@href, "tel:")]/@href,"tel:")').get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "03 233.33.00")

        email = response.xpath('//div[@class="realtor--info"]/p/a[contains(@href, "mailto:")]/@href').get()
        if email:
            item_loader.add_value("landlord_email", email.replace("mailto:", ""))
        else:
            item_loader.add_value("landlord_email", "info@walls.be")

        address = response.xpath(
            "normalize-space(//div[@class='item--street']/h1/a/text())"
        ).extract_first()
        if address:
            lat = response.xpath("//div[@id='map-canvas']/@data-lat").extract_first()
            if lat not in address:
                item_loader.add_value("address", address.replace("&UpperRightArrow;","").strip())
                item_loader.add_value("zipcode", split_address(zip_city, "zip"))
                city = split_address(zip_city, "city")
                if city is not None:
                    item_loader.add_value("city", city.lstrip().rstrip())
            else:
                item_loader.add_value('address', "Antwerpen")
                item_loader.add_value('city', "Antwerpen")
        yield item_loader.load_item()


def split_address(address, get):
    zip_code = "".join(filter(lambda i: i.isdigit(), address))
    if len(address.split(zip_code)) > 1:
        city = address.split(zip_code)[1]
    else:
        city = None
    if get == "zip":
        return zip_code
    else:
        return city

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "gelijkvloerse" in p_type_string.lower() or "detached" in p_type_string.lower() or "duplex" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None