# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import math
import dateparser 


class MySpider(Spider):
    name = "vastgoedunicum_max_immo"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source="Vastgoedunicummaximmo_PySpider_belgium_nl"
    def start_requests(self):
        start_urls = [
            {"url": "http://vastgoedunicum.be/nl/te-huur/panden-te-huur/?type=5&city=&price-range=", "property_type": "house"},
            {"url": "http://vastgoedunicum.be/nl/te-huur/panden-te-huur/?type=1&city=&price-range=", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):

        for item in response.xpath(
            "//a[contains(.,'Meer')]/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        title = " ".join(
            response.xpath("//div[contains(@class,'container')]/h2/text()").extract()
        )
        item_loader.add_value("title", title)
        desc = "".join(response.xpath("//div[@class='description']/text()").extract())
        item_loader.add_value("description", desc.lstrip().rstrip())

        price = response.xpath(
            "//tr[@class='even']//td[contains(.,'Prijs')]//following-sibling::td//text()"
        ).get()
        if price:
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath(
            "//table[@class='financial detail-fields']//tr[./td[contains(.,'borg')]]/td[2]/text()"
        ).extract_first()
        if deposit:
            deposit = deposit.split("€")[1].replace(",", ".").strip(".00").strip()
            if deposit.strip():
                item_loader.add_value("deposit", deposit)

        utilities = response.xpath(
            "//table[@class='financial detail-fields']//tr[./td[.='Gemeenschappelijke kosten']]/td[2]/text()"
        ).get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0])

        # ref = response.xpath("//div[@class='property-ref']/text()").get()
        # ref = ref.split(":")[1]
        # item_loader.add_value("external_id", ref)

        square = response.xpath(
            "//tr[@class='even']//td[contains(.,'Woonoppervlakte')]//following-sibling::td//text()"
        ).get()
        if square:
            square =  square.split("m²")[0]
            item_loader.add_value("square_meters",square)
        else:
            square = response.xpath(
            "//tr[@class='odd']//td[contains(.,'Woonoppervlakte')]//following-sibling::td//text()"
        ).get()
        if square:
            square =  square.split("m²")[0]
            item_loader.add_value("square_meters",square)

        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_xpath(
            "floor",
            "//table[@class='construction detail-fields']//tr[./td[.='Aantal verdiepingen']]/td[@class='value']/text()",
        )

        room_count = response.xpath(
            "//td[contains(.,'Aantal slaapkamer')]/following-sibling::td/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        item_loader.add_xpath(
            "bathroom_count",
            "//td[contains(.,'badkamer')]/following-sibling::td/text()",
        )
        terrace = response.xpath(
            "//table[@class='indeling detail-fields']//tr[./td[.='Terras']]/td[3]/text()"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath(
            "//table[@class='overall detail-fields']//tr[./td[.='Aantal parkeerplaatsen']]/td[@class='value']/text()"
        ).get()
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("parking", True)
            elif "Yes" in terrace:
                item_loader.add_value("parking", True)
            elif "No" in terrace:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", False)

        elevator = response.xpath(
            "//table[@class='comfort detail-fields']//tr[./td[.='Lift']]/td[@class='value']/text()"
        ).get()
        if elevator:
            if "ja" in elevator:
                item_loader.add_value("elevator", True)
            elif "Yes" in elevator:
                item_loader.add_value("elevator", True)
            elif "No" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", False)

        images = [response.urljoin(x) for x in response.xpath("//a[@class='picture-lightbox']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        # else:
        #     images = [
        #     response.urljoin(x)
        #     for x in response.xpath(
        #             "//div[@class='main-picture']//li//img/@src"
        #         ).extract()
        #     ]
        #     if images:
        #         item_loader.add_value("images", images)
        dishwasher = response.xpath(
            "//table[@class='indeling detail-fields']//tr[./td[contains(.,'Keuken')]]/td[3]/text()"
        ).get()
        if dishwasher:
            if "dishwasher" in dishwasher:
                item_loader.add_value("dishwasher", True)

        item_loader.add_xpath(
            "energy_label",
            "normalize-space(//tr[td[.='Energieklasse label']]/td[@class='value']/text())",
        )

        address = response.xpath(
            "normalize-space(//tr[contains(.,'Adres')]/td[2]/text())"
        ).get()
        zip_city = response.xpath(
            "normalize-space(//tr[contains(.,'Gemeente')]/td[2]/text())"
        ).get()

        item_loader.add_value("address", address + " " + zip_city)
        item_loader.add_value("zipcode", split_address(zip_city, "zip"))
        item_loader.add_value("city", split_address(zip_city, "city"))

        phone = response.xpath(
            "//div[@class='col-sm-4 office-block']/p/span[@class='tel']/text()"
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone)

        email = response.xpath(
            "//div[@class='col-sm-4 office-block']/p/strong/a/text()"
        ).get()
        if email:
            item_loader.add_value("landlord_email", email)
        item_loader.add_value("landlord_name", "Vastgoed Unicum")
        available_date=response.xpath("//tr[contains(.,'Vrij op')]/td[2]/text()").get()

        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        item_loader.add_xpath("latitude", "//div[@id='map']/@data-geolat")
        item_loader.add_xpath("longitude", "//div[@id='map']/@data-geolong")
        yield item_loader.load_item()


def split_address(address, get):
    temp = address.strip().split(" ")[1]
    zip_code = "".join(filter(lambda i: i.isdigit(), temp))
    city = address.split(" ")[0]
    if city.isdigit():
        city = address.strip().split(" ")[1]
        zip_code = address.strip().split(" ")[0]

    if get == "zip":
        return zip_code
    else:
        return city
