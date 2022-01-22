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
from datetime import datetime
import re


class MySpider(Spider):
    name = "nadimmo"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source='Nadimmo_PySpider_belgium_nl'

    custom_settings = {
        "LOG_LEVEL":"DEBUG",
    }
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.nadimmo.be/Rechercher/Locations/Type-01=Maison", "property_type": "house"},
            {"url": "https://www.nadimmo.be/Rechercher/Locations/Type-03=Appartement", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='grid']/div[contains(@class,'list-item')]"):
            follow_url = response.urljoin(item.xpath("./a/@href").extract_first())
            if follow_url:
                address = item.xpath(".//h4/text()").extract_first()
                if "javascript" not in follow_url:
                    yield Request(
                        follow_url, callback=self.populate_item, 
                        meta={"address": address, 'property_type': response.meta.get('property_type')}
                    )
        
        pagination = response.xpath("//li[@class=' hidden-xs']/a[.='»']/@href").extract_first()
        if pagination:
            yield Request(
                pagination,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')},
            )
        

    

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        address = response.meta.get("address")

        item_loader.add_value("external_source", "Nadimmo_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        prop_control = response.xpath("//div[@id='documentsModal']/following-sibling::div[1]//h2/text() | //h2/following-sibling::p/parent::div/h2/text()").get()
        property_type = response.meta.get('property_type')
        
        if prop_control and ('apartement' in prop_control.lower() or 'apartment' in prop_control.lower()):
            property_type = 'apartment'
        item_loader.add_value("property_type", property_type)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        zipcode = response.xpath("//iframe/@src[contains(.,'maps/')]").get()
        if zipcode:
            zipcode = zipcode.split("q=")[-1].strip().split(" ")[0]
            if zipcode.isdigit(): item_loader.add_value("zipcode", zipcode)
        
        description = "".join(response.xpath("//div[@class='row']/following-sibling::p//text()").extract())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description",description.strip())

        if 'no pet' in description.lower():
            item_loader.add_value("pets_allowed", False)

        square = response.xpath("//table[@class='table table-striped']//tr[./td[.='Net living area']]/td[2]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
        
        room = response.xpath("//div[@class='row']//div[i[contains(@class,'fa-bed')]]/text()[normalize-space()]").extract_first()
        if room:    
            item_loader.add_value("room_count",room.split("bedroom")[0].strip())
        elif "studio" in description:
            item_loader.add_value("room_count","1")

        bathroom = response.xpath("//td[contains(.,'Bathroom')]/following-sibling::td/text()").get()
        if bathroom:    
            item_loader.add_value("bathroom_count", bathroom.strip())
        
        price = response.xpath(
            "//table[@class='table table-striped']//tr[./td[.='Price']]/td[2]/text()[contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[0])
        item_loader.add_value("currency", "EUR")
        
        date = response.xpath(
            "//tr[./td[.='Availability']]/td[2]/text()[contains(.,'/')]"
        ).extract_first()
        if date:
            item_loader.add_value(
                "available_date",
                datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d"),
            )

        item_loader.add_value("address", address)
        item_loader.add_value("city", address)

        bathroom_count = response.xpath("//td[contains(.,'Bathrooms')]/following-sibling::td/text() | //td[contains(.,'Badkamer')]/following-sibling::td/text()").get()
        if bathroom_count:
            if "+" in bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split("+")[0].strip())
            else:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
                            
        external_id = response.xpath("//span[contains(.,'Ref')]/b/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        utilities = response.xpath("//td[contains(.,'Charges')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        else:
            utilities = response.xpath("//td[contains(.,'Rental loads')]/following-sibling::td/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€")[0].strip())

        item_loader.add_xpath(
            "floor",
            "//table[@class='table table-striped']//tr[contains(.,'Number of floors')]/td[2]/text()",
        )

        terrace = response.xpath("//tr[./td[.='Terrace']]/td[.='Yes']/text()").get()
        if terrace:
            if terrace == "Yes":
                item_loader.add_value("terrace", True)
            elif terrace == "No":
                item_loader.add_value("terrace", False)
        terrace = response.xpath(
            "//table[@class='table table-striped']//tr[./td[.='Furniture' or .='Furnished']]/td[2]/text()"
        ).get()
        if terrace:
            if "Yes" in terrace:
                item_loader.add_value("furnished", True)
            elif "No" in terrace:
                item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//td[contains(.,'Meublé')]/following-sibling::td/text()").get()
            if furnished:
                if furnished.strip().lower() == 'oui':
                    item_loader.add_value("furnished", True)
                elif furnished.strip().lower() == 'non':
                    item_loader.add_value("furnished", False)

        terrace = response.xpath("//tr[@id='contentHolder_parkingZone']/td[2]").get()
        if terrace:
            item_loader.add_value("parking", True)
        else:
            terrace = response.xpath("//tr[td[.='Parking places']]/td[2]").get()
            if terrace:
                item_loader.add_value("parking", True)
            # else:
            #     item_loader.add_value("parking", False)

        terrace = response.xpath(
            "//tr[@id='contentHolder_interiorList_detailZone_3']/td[.='Yes']"
        ).get()
        if terrace:
            if terrace == "Yes":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        energy = response.xpath("//img[@alt='peb']/@src").get()
        if energy:
            item_loader.add_value(
                "energy_label",
                energy.split(
                    "https://bbl.evosys.be/Virtual/ETTW3b/v3/images/PEB/large/NL/"
                )[1]
                .split(".")[0]
                .upper(),
            )
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='carousel-inner']/div/div/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
    
        item_loader.add_value("landlord_email", " info@nadimmo.be")

        item_loader.add_value("landlord_name", "NADIMMO Ltd")
        item_loader.add_value("landlord_phone", "00322-280 03 03")

        dishwasher = response.xpath(
            "//tr[contains(.,'Indoor facilities')]/td[2]/text()[contains(.,'Dishwasher')]"
        ).extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = response.xpath("//tr[contains(.,'Indoor facilities')]/td[2]/text()[contains(.,'Washing machine')]").extract_first()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        

        yield item_loader.load_item()
