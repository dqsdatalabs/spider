# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
import math
class MySpider(Spider):
    name = 'agencedesallees_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agencedesallees_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agence-des-allees.com/properties/?filter_location=&filter_sublocation=&filter_sub_sublocation=&filter_type=4&filter_contract_type=210&filter_price_from=&filter_price_to=&filter_area_from=", "property_type": "apartment"},
            {"url": "https://www.agence-des-allees.com/properties/?filter_location=&filter_sublocation=&filter_sub_sublocation=&filter_type=46&filter_contract_type=210&filter_price_from=&filter_price_to=&filter_area_from=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
       
        for follow_url in response.xpath("//div[contains(@class,'property span9') and div[not (contains(.,'LOUÉ'))]]//h2/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
       
        next_page = response.xpath("//ul[@class='unstyled']/li[@class='active']/following-sibling::li[1]/a/@href").get()
        if next_page: 
            yield Request(next_page, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@class='property-detail']/p/text()").extract())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            if "NON MEUBLEE" in desc.upper():
                item_loader.add_value("furnished", False)
            elif "MEUBLEE" in desc.upper():
                item_loader.add_value("furnished", True)


        available_date = "".join(response.xpath("//div[@class='property-detail']/p[contains(.,'Libre au')]//text()").extract())
        if available_date:
            try:
                match = re.search(r'(\d+/\d+/\d+)',available_date.split("Libre au")[1])
                if match:
                    new_format = dateparser.parse(match.group(1).strip(), languages=['en']).strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", new_format)
            except:
                pass
        price = response.xpath("//div[@class='property-detail']//div[@class='span3']//tr[./th[.='Prix:']]/td/text()[contains(.,'€')]").get()
        if price:
            item_loader.add_value(
                "rent_string", price.strip())
            # item_loader.add_value("currency", "EUR")
        
        # utilities = response.xpath("//div[@class='property-detail']/p/strong[contains(.,'Charges')]/text()").get()
        # if utilities:
        #     item_loader.add_value("utilities", utilities.split(":")[1].strip().split("€")[0])
        
        item_loader.add_xpath(
            "external_id", "//div[@class='property-detail']//div[@class='span3']//tr[./th[.='Référence:']]/td/strong/text()"
        )

        square = response.xpath(
            "//div[@class='property-detail']//div[@class='span3']//tr[./th[.='Surface:']]/td/text()"
        ).get()
        if square:
            square_meters = square.split("m")[0]
            item_loader.add_value(
                "square_meters", str(math.ceil(float(square_meters)))
            )
        room_count = response.xpath(
            "//div[@class='property-detail']//div[@class='span3']//tr[./th[.='Chambres:']]/td/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//h1[@class='page-header']/text()[contains(.,'PIECES')]").get()
            if room_count:
                room_count = room_count.split("PIECES")[0].strip().split(" ")[-1]
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath("//h1[@class='page-header']/text()[contains(.,'PIÈCE')]").get()
                if room_count:
                    room_count = room_count.split("PIÈCE")[0].strip().split(" ")[-1]
                    item_loader.add_value("room_count", room_count)
        
        address = response.xpath("//h1[@class='page-header']/text()").get()
        if address:
            if "PIECES" in address or "PI\u00c8CES" in address:
                if "-" in address:
                    if "PIECES -" in address:
                        item_loader.add_value("address", address.split("-")[1].strip())
                    elif "\u00b2-" in address:
                        item_loader.add_value("address", address.split("-")[1].strip())
                    else:
                        item_loader.add_value("address", address.split("-")[0].strip())
                else:
                    if "PIECES " in address:
                        address = response.xpath("substring-after(//h1[@class='page-header']/text(),'PIECES ')").get()
                        item_loader.add_value("address", address.strip())
                    else:
                        address = response.xpath("substring-before(//h1[@class='page-header']/text(),'PIECES')").get()
                        item_loader.add_value("address", address.strip())
            else:
                item_loader.add_value("address", address)

        item_loader.add_value("city", item_loader.get_collected_values('address'))
        
        item_loader.add_xpath("zipcode", "//div[@class='property-detail']//div[@class='span3']//tr[./th[.='Ville:']]/td/text()")
            

        energy_label = response.xpath("//div[@id='execphp-2']/div/img/@src").get()
        if energy_label:
            energy = energy_label.split("/")[-1].split(".")[0].strip()
            if energy.isalpha():
                item_loader.add_value("energy_label", energy)
        terrace = response.xpath(
            "//div[@class='property-detail']//ul[@class='span2']/li/text()[contains(.,'Terrasse')]"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath(
            "//div[@class='interior']/small[contains(.,'Furnished')]"
        ).get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath(
            "//div[@class='property-detail']//ul[@class='span2']/li/text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath(
            "//div[@class='property-detail']//ul[@class='span2']/li/text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        parking = response.xpath("//h1/text()").get()
        if parking:
            if "parking" in parking.lower():
                item_loader.add_value("parking", True)
        
        item_loader.add_xpath("energy_label", "normalize-space(//div[@class='rightcontainer']/div[@class='row']/div[contains(.,'Energylabel')]/following-sibling::div/text())")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='carousel property']/div[@class='content']//ul/li/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_xpath("landlord_phone", "//div[@class='agent clearfix']/div[@class='phone']/text()")
        item_loader.add_xpath("landlord_name", "//div[@class='agent clearfix']/div[@class='name']/a/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='agent clearfix']/div[@class='email']/a/text()")


        yield item_loader.load_item()