# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser


class MySpider(Spider):
    name = 'thijssenmakelaardij_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = [
        "https://www.thijssenmakelaardij.nl/aanbod/woningaanbod/huur/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//li[contains(@class,'al2woning')]"):
            status = item.xpath(
                ".//span[contains(@class,'objectstatusbanner')]/text()").get()
            if status and ("verhuurd" in status.lower() or "onder" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.thijssenmakelaardij.nl/aanbod/woningaanbod/huur/pagina-{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page": page+1})

    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value(
            "external_source", "Thijssenmakelaardij_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath(
            "//span[contains(.,'Soort object')]/following-sibling::span[1]/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath(
                "//div[@class='ogContent']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value(
                    "property_type", get_p_type_string(f_text))
            else:
                return

        title = "".join(response.xpath("//title//text()").get())
        if title:
            item_loader.add_value("title", title)

        address = "".join(response.xpath(
            "//div[contains(@class,'address')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)

        utilities = "".join(response.xpath(
            "//div[@id='Omschrijving']/text()[contains(.,'servicekosten')]").getall())
        if utilities:
            uti = utilities.split("servicekosten")[1].split(",")[0]
            if "€" in uti:
                item_loader.add_value("utilities", uti.split("€")[1].strip())

        item_loader.add_value("external_id", response.url.split(
            "-")[1].split("-")[0].strip())

        city = response.xpath("//span[@class='locality']/text()").get()
        if city:
            item_loader.add_value("city", city)

        zipcode = response.xpath("//span[@class='postal-code']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath(
            "//span/span[contains(.,'Huurprij')]/following-sibling::span/text()").get()
        if rent and "€" in rent:
            price = rent.split("€")[1].split(",-")[0].strip().replace(".", "")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        square_meters = "".join(response.xpath(
            "//span/span[contains(.,'Woonoppervlakte')]/following-sibling::span//text()").getall())
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath(
            "//span/span[contains(.,'slaapkamer')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        desc = " ".join(response.xpath(
            "//div[@id='Omschrijving']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        available_date = "".join(response.xpath(
            "//span[contains(@class,'aanvaarding')]/span[@class='kenmerkValue']/text()[not(contains(.,'Direct') or contains(.,'In overleg'))]").getall())
        if available_date:
            try:
                date2 = available_date.split("Per")[1].strip()
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
            except:
                pass
        images = [x for x in response.xpath(
            "//div[@class='detailFotos']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath(
            "//span/span[contains(.,'Gelegen op')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip().split(" ")[
                                  0].replace("e", ""))
        else:
            floor = response.xpath(
                "//span/span[contains(.,'woonlagen')]/following-sibling::span/text()").get()
            if floor:
                item_loader.add_value("floor", floor.strip().split(" ")[0])

        if "Borg:" in desc:
            deposit = desc.split("Borg:")[1].split(
                ",-")[0].replace("\u20ac", "").strip()
            item_loader.add_value("deposit", deposit.replace(".", ""))
        else:
            deposit = "".join(response.xpath(
                "//span[contains(@class,'waarborgsom')]/span[@class='kenmerkValue']/text()").getall())
            if deposit:
                deposit = deposit.split(",")[0].strip().replace(".", "")
                item_loader.add_value("deposit", deposit.strip())

        latitude_longitude = response.xpath(
            "//script[contains(.,'longitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
               '"latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split(
                '"longitude":')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        parking = response.xpath(
            "//span/span[contains(.,'Parkeerfaciliteiten')]/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        name = response.xpath("//h4/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        else:
            item_loader.add_value(
                "landlord_name", "Thijssen Makelaardij Zaandam")

        email = response.xpath("//a[@class='makelaarEmail']/@href").get()
        if email:
            item_loader.add_value(
                "landlord_email", email.split(":")[1].strip())
        else:
            item_loader.add_value(
                "landlord_email", "zaandam@thijssenmakelaardij.nl")

        phone = response.xpath("//a[contains(@class,'tel')]//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "075-6168669")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None
