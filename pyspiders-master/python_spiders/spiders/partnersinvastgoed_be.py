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
import dateparser 


class MySpider(Spider):
    name = "partnersinvastgoed_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.partnersinvastgoed.be/te-huur?searchon=list&sorts=Dwelling&transactiontype=Rent", "property_type": "house"},
            {"url": "https://www.partnersinvastgoed.be/te-huur?searchon=list&sorts=Flat&transactiontype=Rent", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath(
            "//div[contains(@class,'row switch-view-container')]/a[contains(@class,'pand-wrapper')]/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        pagination = response.xpath("//div[contains(@class,'paging-next')]/a/@href").extract_first()
        if pagination:
            url_joined = response.urljoin(pagination)
            yield Request(url=url_joined,
                             callback=self.parse,
                             meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Partnersinvastgoed_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title =response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\r","").replace("\n","").strip())

        item_loader.add_xpath(
            "description", "//div[@class='row tab description']/div/p"
        )
        price = response.xpath("//tr[./td[.='Prijs:']]/td[2]").extract_first()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].split("/")[0].split("<")[0]
            )
            item_loader.add_value("currency", "EUR")

        external_id = response.url.split('detail/')[1].split('/')[1].split('?')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        bathroom_count = response.xpath("//tr[./td[.='Badkamers:']]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        energy_label = response.xpath("//tr[./td[.='EPC Index:']]/td[2]/text()").get()
        if energy_label:
            energy_label = float(energy_label.upper().split('K')[0].strip().replace(',', '.'))
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

        desc = " ".join(response.xpath("//div[@class='row tab description']/div/p/text()").getall()).strip()
        if desc:
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)
            if 'vaatwasser' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'balkon' in desc.lower():
                item_loader.add_value("balcony", True)

        pets_allowed = response.xpath("//tr[./td[.='Huisdieren toegelaten:']]/td[2]/text()").get()
        if pets_allowed:
            if pets_allowed.strip().lower() == 'ja':
                item_loader.add_value("pets_allowed", True)
            elif pets_allowed.strip().lower() == 'neen':
                item_loader.add_value("pets_allowed", False)

        address = " ".join(
            response.xpath("//tr[./td[.='Adres:']]/td[2]/text()").extract()
        )
        if address:
            item_loader.add_value("address", address.replace("/"," / "))
            # item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        zipcode = response.xpath("//h1//text()").get()
        if zipcode:
            zipcode=re.findall("\d{4}",zipcode)
            item_loader.add_value("zipcode", zipcode)

        square = response.xpath(
            "//tr[./td[.='Bewoonbare opp.:']]/td[2]/text()[. != '0 m²']"
        ).extract_first()
        if square:
            square = square.split("m²")[0].strip()
            item_loader.add_value("square_meters", square)

        prop_studio = response.xpath("//td[contains(.,'Type:')]/following-sibling::td/text()").get()
        if prop_studio and "studio" in prop_studio.lower(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta.get("property_type"))

        room = response.xpath("//tr[./td[.='Slaapkamers:']]/td[2]/text()").get()
        if room:
            if "studio" in item_loader.get_collected_values("property_type") and "0" in room:
                item_loader.add_value("room_count", "1")
            else:
                if "0" not in room:
                    item_loader.add_value("room_count", room)
        floor = response.xpath("//tr[./td[.='Op verdieping:']]/td[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        available_date = response.xpath(
            "//tr[./td[.='Beschikbaar vanaf:']]/td[2]/text()[. != 'Onmiddellijk' and . !='In overleg']"
        ).extract_first()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d/%m/%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='owl-carousel']//a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        terrace = response.xpath("//tr[./td[.='Terras:']]/td[2]/text()").get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("terrace", True)
            elif terrace == "Nee":
                item_loader.add_value("terrace", False)
            elif response.xpath("//tr[./td[.='Terras:']]/td[2]/text()").get():
                item_loader.add_value("terrace", True)

        terrace = response.xpath("//tr[./td[.='Parking:']]/td[2]/text()").get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("parking", True)

        terrace = response.xpath(
            "//tr[./td[.='Lift:']]/td[2]/text()/text()[.='Ja']"
        ).get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("elevator", True)
            if terrace == "Nee":
                item_loader.add_value("elevator", False)

        phone = response.xpath('//div[@class="col-sm-4"][3]/a/@href').get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))

        email = response.xpath('//div[@class="col-sm-4"][1]/a/@href').get()
        if email:
            item_loader.add_value("landlord_email", email.replace("mailto:", ""))

        item_loader.add_value("landlord_name", "Partners in Vastgoed")
        script = " ".join(response.xpath("//script[5]/text()").extract())

        script = (
            script.strip()
            .split("PR4.detail.enableMap({")[1]
            .strip()
            .split("});")[0]
            .strip()
        )

        item_loader.add_value(
            "latitude", script.strip().split(",")[0].split(" ")[1]
        )
        item_loader.add_value(
            "longitude",
            script.replace("            ", " ").split(",")[1].split(" ")[2],
        )
        yield item_loader.load_item()


def split_address(address, get):
    # temp = address.split(" ")[0]
    zip_code = "".join(filter(lambda i: i.isdigit(), address))
    city = address.split(" ")[-1]

    if get == "zip":
        return zip_code
    else:
        return city
