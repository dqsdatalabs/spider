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
import math
import dateparser
import re


class MySpider(Spider):
    name = "pajota"
    download_timeout = 60
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = 'Pajota_PySpider_belgium_nl'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.pajota.be/nl/te-huur/woningen", "property_type": "house"},
            {"url": "https://www.pajota.be/nl/te-huur/appartementen", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse_listing,
                             meta={'property_type': url.get('property_type')})

    def parse_listing(self, response):

        for item in response.xpath(
            "//section[@id='properties__list']/ul[contains(@class,'grid__center')]/li"
        ):
            url = item.xpath(
                "./a[contains(@class,'property-contents')]/@href"
            ).extract_first()
            
            if "referenties" not in url:
                yield Request(url, callback=self.parse_detail, meta={'property_type': response.meta.get('property_type')})


    def parse_detail(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Pajota_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='property__detail-container']/h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        prop_type = response.xpath("//div[@class='category']//text()").extract_first()
        if "Studio" in prop_type:
            item_loader.add_value("property_type", "Studio")
        else:
            item_loader.add_value("property_type", response.meta.get("property_type"))

        description = " ".join(response.xpath("//div[@class='property__description']//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc)

        price = "".join(
            response.xpath("//div[@class='price'][contains(., '€')]").extract()
        )
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].split("p")[0].split(",")[0]
            )
        item_loader.add_value("currency", "EUR")

        item_loader.add_xpath(
            "external_id", "normalize-space(//div[@class='reference']/text())"
        )
        utilities = "".join(
            response.xpath("//div[@class='details-content']//div[./dt[.='kosten']]/dd//text()[1]").extract()
        )
        if utilities:
            numbers = re.findall(r'\d+(?:\.\d+)?', utilities)
            if numbers:
                item_loader.add_value("utilities", numbers[0])
        square = response.xpath(
            "//div[@class='details-content']//div[./dt[.='bewoonbare opp.']]/dd/text()"
        ).get()
        if square:
            square = square.split("m²")[0].strip()
            if "," in square:
                square = square.replace(",", ".")
                square = math.ceil(float(square))
            item_loader.add_value(
                "square_meters", str(square)
            )
        
        room_count = response.xpath(
            "//div[@class='details-content']//div[./dt[.='slaapkamers']]/dd/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif not room_count:
            if "Studio" in prop_type:
                item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("//div[@class='details-content']//div[./dt[.='badkamers']]/dd/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())


        

        address = "".join(
            response.xpath("//div[@class='address']/text()").extract()
        )
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value(
                "city", split_address(address, "city").strip()
            )
        
        available_date = response.xpath(
            "//div[@class='details-content']//div[./dt[.='beschikbaarheid']]/dd/text()"
        ).get()
        if available_date:
            if available_date != "Onmiddellijk":
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d %B %Y"]
                )
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    
                    item_loader.add_value("available_date", date2)

        item_loader.add_xpath(
            "floor",
            "//div[@class='details-content']//div[./dt[.='verdieping']]/dd/text()",
        )

        terrace = response.xpath(
            "//div[@class='details-content']//div[./dt[.='terras']]/dd//text()"
        ).get()
        if terrace:
            if terrace.lower() == "ja":
                item_loader.add_value("terrace", True)
            elif terrace == "nee":
                item_loader.add_value("terrace", False)
            
            
        terrace = response.xpath(
            "//div[@class='details-content']//div[./dt[.='gemeubeld']]/dd//text()"
        ).get()
        if terrace:
            if terrace.lower() == "ja":
                item_loader.add_value("furnished", True)
            elif terrace == "nee":
                item_loader.add_value("furnished", False)
        terrace = response.xpath(
            "//div[@class='details-content']//div[./dt[.='buitenparking' or .='garages / parking' or .='binnenparking']]/dd//text()"
        ).get()
        if terrace:
            if terrace == "nee":
                park = response.xpath("//div[@class='details-content']//div[./dt[.='buitenparking']]/dd//text()").get()
                if park and park.lower() == "ja":
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        terrace = response.xpath(
            "//div[@class='details-content']//div[./dt[.='lift']]/dd/text()"
        ).get()
        if terrace:
            if terrace == "ja":
                item_loader.add_value("elevator", True)
            elif terrace == "nee":
                item_loader.add_value("elevator", False)
        terrace = response.xpath(
            "//div[@class='details-content']//div[./dt[.='zwembad']]/dd//text()"
        ).get()
        if terrace:
            if terrace.lower() == "ja":
                item_loader.add_value("swimming_pool", True)
            else:
                item_loader.add_value("swimming_pool", False)
        balcony = response.xpath(
            "//div[@class='details-content']//div[./dt[.='balkon']]/dd//text()"
        ).get()
        if balcony:
            balcony_title = response.xpath("//div[@class='property__detail-container']/h1//text()[contains(.,'balkon')]").get()
            if balcony.lower() == "ja" or balcony_title:
                item_loader.add_value("balcony", True)
            elif balcony == "nee":
                item_loader.add_value("balcony", False)

        energy = response.xpath(
            "//div[@class='details-content']//div[./dt[.='epc']]/dd/span/@class"
        ).get()
        if energy:
            item_loader.add_value(
                "energy_label", energy.split(" ")[1].split("_")[1].upper()
            )
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//section[@id='property__photos']//picture/source/@srcset"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude_longitude = response.xpath("//section[@id='property__map']/script/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat = ')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('lng = ')[1].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        phone = response.xpath(
            "normalize-space(//div[@class='field-name-property-contact-phone field-type-text']//a)"
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))

        name = response.xpath("//div[@class='name']//text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        item_loader.add_value("landlord_email", "info@pajota.be")
        item_loader.add_value("landlord_phone", "02 466 05 75")
        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1]

        if get == "zip":
            return zip_code
        else:
            return city
