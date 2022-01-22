# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import math
import dateparser

class MySpider(Spider):
    name = "ifaservice"
    download_timeout = 60
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'

    def start_requests(self):

        start_urls = [
            {"type": "1", "property_type": "house"},
            {"type": "2", "property_type": "apartment"},
        ]  # LEVEL 1
        for url in start_urls:
            data = {
                "PropertyGoalId": "4",
                "Types": f"{url.get('type')}",
                "offset": "0",
            }
            yield FormRequest(
                "https://www.ifacservice.be/loadOnScroll",
                body=json.dumps(data),
                formdata=data,
                dont_filter=True,
                callback=self.parse_listing,
                meta={"offset": 0, 
                    'property_type': url.get('property_type'),
                    'type':url.get('type')},
            )

    def parse_listing(self, response):
        data = json.loads(response.body)
        if data:
            for item in data:
                url = item["Url"]
                yield Request(url, 
                                callback=self.parse_detail, 
                                meta={'property_type': response.meta.get('property_type')})

            offset = response.meta.get("offset")
            offset += 12
            data = {
                "PropertyGoalId": "4",
                "Types": f"{response.meta.get('type')}",
                "offset": f"{str(offset)}",
            }
            yield FormRequest(
                "https://www.ifacservice.be/loadOnScroll",
                body=json.dumps(data),
                formdata=data,
                dont_filter=True,
                callback=self.parse_listing,
                meta={"offset": offset, 
                'type':response.meta.get('type'),
                'property_type': response.meta.get('property_type')},
            )

    def parse_detail(self, response):
        item_loader = ListingLoader(response=response)

        rented = response.xpath("//span[@class='labelwrap']/span[contains(.,'Verhuurd')]//text()").get()
        if rented:
            return
        item_loader.add_value("external_source", "Ifaservice_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        ref = response.xpath("//span[@class='ref']/text()").get()
        ref = ref.split(" ")[1]
        item_loader.add_value("external_id", ref)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_xpath("title", "//div[@class='tab']//div[@class='col']/address/text()")
        desc = "".join(response.xpath("//div[@class='txt']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.rstrip().lstrip())
            if "balkon " in desc:
                item_loader.add_value("balcony", True)
            if "vaatwasser" in desc:
                item_loader.add_value("dishwasher", True)
            if "terras" in desc:
                item_loader.add_value("terrace", True)
            if "garage" in desc.lower() or "parkeerplaats" in desc:
                item_loader.add_value("parking", True)

        price = response.xpath("//span[@class='price']/text()").extract_first()
        if price:
            if "+" in price:
                rent = price.split("+")[0].strip()
                item_loader.add_value("rent_string", rent)
                utilities = price.split("+")[1].strip()
                item_loader.add_value("utilities", utilities.replace("€","").strip())
            else:
                item_loader.add_value("rent_string", price)
        # item_loader.add_value("currency", "EUR")

        address = response.xpath(
            "//div[@class='tab']//div[@class='col']/address/text()"
        ).get()
        item_loader.add_value("address", address.strip())
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city").strip())

        available_date = response.xpath(
            "//div[@id='info']//ul/li[./strong[.='Beschikbaar:']]/text()"
        ).get()

        if available_date and available_date.replace(" ","").isalpha() != True:
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        square = response.xpath("//div[@class='grid']//ul/li[contains(.,'Woonoppervlakte')]/text()").get()
        if square:
            if "," in square:
                square = square.replace(",", ".")
                square = math.ceil(float(square))
            item_loader.add_value(
                "square_meters", str(square)
            )
        else:
            square = response.xpath(
            "//div[@class='grid' and ./div[.='Living:']]/div[2]/text()"
        ).extract_first()
            if square:
                square = square.split("m²")[0].strip()
                if "," in square:
                    square = square.replace(",", ".")
                    square = math.ceil(float(square))
                item_loader.add_value(
                    "square_meters", str(square)
                )

        # floor = response.xpath("//h3[contains(.,'Verdieping')]/text()").get()
        # if floor:
        #     floor = floor.split(" ")[1]
        #     item_loader.add_value("floor", floor)
        
        room = response.xpath(
            "//div[@class='col']/h3[.='Algemene informatie']/following-sibling::ul/li[./strong[.='Aantal slaapkamers:']]/text()"
        ).get()
        if room:
            if ":" in room:
                room = room.split(":")[1]
            item_loader.add_value("room_count", room)
        elif not room:
            if desc and "studio" in desc.lower():
                item_loader.add_value("room_count", "1")
        bath = response.xpath(
            "//div[@class='col']/h3[.='Algemene informatie']/following-sibling::ul/li[./strong[.='Aantal badkamers:']]/text()"
        ).get()
        if bath:
            if ":" in bath:
                room = room.split(":")[1]
            item_loader.add_value("bathroom_count", bath)
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='gallery']//a/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        floor_images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='documenten']//li[contains(.,'plan')]/a/@href"
            ).extract()
        ]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)
        terrace = response.xpath(
            "//div[@class='col']/ul/li[.//div[.='Terras:']]//strong"
        ).get()
        if terrace:
            if terrace is not None:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
        
        dishwasher = response.xpath(
            "//div[@id='indeling']//ul/li[.//strong[.='Keuken:']]/div/div[2]/text()[contains(.,'Kookplaat')]"
        ).get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        terrace = response.xpath(
            "//div[@class='col']/ul/li[.//div[.='Garage:']]//strong"
        ).get()
        if terrace:
            if terrace is not None:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        elevator = response.xpath(
            "//div[@id='info']//ul/li[./strong[.='Lift:']]/text()"
        ).get()
        if elevator:
            if elevator == "Ja":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        phone = response.xpath(
            '//div[@class="col info"]/p/a[contains(@href, "tel:")]/@href'
        ).get()
        if phone:
            item_loader.add_value(
                "landlord_phone", phone.replace("tel:", "")
            )
        email = response.xpath(
            '//div[@class="col info"]/p/a[contains(@href, "mailto:")]/@href'
        ).get()
        if phone:
            item_loader.add_value(
                "landlord_email", email.replace("mailto:", "")
            )
        item_loader.add_value("landlord_name", "IFAC SERVICE BV")

        script = response.xpath("normalize-space(//script[2]/text())").get()
        if script:
            script = script.split("map.setCenter(")[1].split("); }")[0]
            item_loader.add_value(
                "latitude", script.split(",")[0].split(" ")[1]
            )
            item_loader.add_value(
                "longitude",
                script.split(",")[1].split(" ")[2].split("}")[0],
            )
        energy = response.xpath(
            "//div[@class='col']/h3[.='Algemene informatie']/following-sibling::ul/li[./strong[.='Energielabel:']]/text()"
        ).get()
        if energy:
            item_loader.add_value("energy_label", energy)
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
