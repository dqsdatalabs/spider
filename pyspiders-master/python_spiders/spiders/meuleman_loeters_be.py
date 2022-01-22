# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = "meuleman_loeters_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.meuleman-loeters.be/te-huur?searchon=list&sorts=Flat&transactiontype=Rent", "property_type": "apartment"},
            {"url": "https://www.meuleman-loeters.be/te-huur?searchon=list&sorts=Dwelling&transactiontype=Rent", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
 
        for item in response.xpath(
            "//div[contains(@class,'switch-view-container')]/a/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        pagination = response.xpath("//div[contains(@class,'paging-next')]/a/@href").extract_first()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Meulemanloeters_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1[@class='col-md-6']/text()").extract_first().strip()
        item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        square = response.xpath(
            "//tr[td[.='Bewoonbare opp.:' or .='Perceeloppervlakte:' or .='Gevelbreedte:'] or contains(.,'Oppervlakte:')]/td[2]/text()"
            ).get()
        room_count = response.xpath("//tr[./td[.='Slaapkamers:']]/td[2]/text()").extract_first()
        
        desc = "".join(
            response.xpath("//div[@class='row tab description']/div/p").extract()
        )
        if desc:
            item_loader.add_value("description", desc.strip())
        if " wasmachine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if "vaatwasmachine" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "balkon" in desc.lower():
            item_loader.add_value("balcony", True)

        item_loader.add_value("external_link", response.url)
        
        rent = response.xpath("//tr[td[.='Prijs:']]/td[2]/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.split("/")[0])
            
        item_loader.add_xpath("external_id", "//tr[td[.='Referentie:']]/td[2]")
    
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
        if room_count:    
            item_loader.add_value("room_count", room_count)
        
        bathroom=response.xpath("//tr[./td[.='Badkamers:']]/td[2]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        available_date = response.xpath("//tr[td[.='Beschikbaar vanaf:']]/td[2]/text()").extract_first()
        if available_date:
            if available_date != "Onmiddellijk":
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d %B %Y"]
                )
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        latlong = response.xpath(
            "//script[@type='application/ld+json'][1]/text()"
        ).extract_first()
        if latlong:
            item_loader.add_value(
                "latitude", latlong.split('{"latitude":"')[1].split('","')[0]
            )
            item_loader.add_value(
                "longitude", latlong.split('"longitude":"')[1].split('","')[0]
            )

        utilities = response.xpath("//tr[th[.='Charges (€) (amount)']]/td").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split('/')[0].strip().replace('\xa0', ''))
        else:
            utilities = response.xpath("//td[contains(.,'Totale kosten')]/following-sibling::td/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split('€')[-1].split('/')[0].strip().replace('\xa0', ''))

        item_loader.add_xpath("floor", "//tr[td[.='Op verdieping:']]/td[2]/text()")
        address = " ".join(
            response.xpath("//tr[td[.='Adres:']]/td[2]/text()").extract()
        )
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))
        
        label=response.xpath("//tr[contains(@class,'energyClass')]/td[2]/text()").get()
        label_value=response.xpath("//tr[contains(.,'Index')]/td[2]/text()").get()
        if label:
            item_loader.add_value(
                "energy_label", label)
        elif label_value:
            label_value=label_value.split(",")[0]
            e_label=energy_label_calculate(label_value)
            item_loader.add_value("energy_label",e_label)
            
        images = [
            response.urljoin(x)
            for x in response.xpath("//div[@class='owl-carousel']//a/@href").extract()
        ]
        item_loader.add_value("images", images)

        terrace = response.xpath(
            "//tr[td[.='Terras:']]/td[2]/text() | //tr[td[.='Balkon:']]/td[2]/text()"
        ).get()
        if terrace:
            if terrace == "Ja" or terrace == "Oui":
                item_loader.add_value("terrace", True)
            elif terrace == "Non":
                item_loader.add_value("terrace", False)

        parking = response.xpath("//tr[td[.='Parking:']]/td[2]/text()").get()
        if parking:
            if 'ja' in parking.lower():
                item_loader.add_value("parking", True)
            elif 'nee' in parking.lower():
                item_loader.add_value("parking", False)
            else:
                try:
                    parking = int(parking)
                    if parking > 0:
                        item_loader.add_value("parking", True)
                    elif parking == 0:
                        item_loader.add_value("parking", False)
                except:
                    pass

        terrace = response.xpath("//tr[td[.='Lift:']]/td[2]/text()").get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("elevator", True)
            elif terrace == "Neen":
                item_loader.add_value("elevator", False)

        phone = response.xpath(
            '//div[@class="row large"]/div/a[contains(@href, "tel:")]/@href'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))

        email = response.xpath(
            '//div[@class="row large"]/div/a[contains(@href, "mailto:")]/text()'
        ).get()
        if email:
            item_loader.add_value("landlord_email", email)

        item_loader.add_value("landlord_name", "Meuleman Loeters")

        yield item_loader.load_item()


def split_address(address, get):
    temp = address.split(" ")[-2]
    zip_code = "".join(filter(lambda i: i.isdigit(), temp))
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