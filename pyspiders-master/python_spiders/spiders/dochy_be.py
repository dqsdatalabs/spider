# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from scrapy import Request, FormRequest
import unicodedata
import re

class MySpider(Spider):
    name = "dochy_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source='Dochy_PySpider_belgium_nl'
    scale_separator ='.'

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "www.dochy.be",
        "Origin": "https://www.dochy.be",
    }
    def start_requests(self):

        start_urls = [
            {"type": "1", "property_type": "house", "value": "villa"},
            {"type": "2", "property_type": "apartment", "value": "appartement"},
            {"type": "3", "property_type": "house", "value": "woning"},
        ]  # LEVEL 1
        for url in start_urls:
            r_type = url.get("value")
            data = {
                "radius":"0",
                "city_zip": "",
                "category": f"{r_type}",
                "price": "" ,
            
            }

            yield FormRequest(
                "https://www.dochy.be/nl/aanbod/te-huur/",
                formdata=data,
                headers=self.headers,
                callback=self.parse,
                meta={'property_type': url.get('property_type'), 'type':url.get('type')},
            )
    
    # 1. FOLLOWING LEVEL 1
    def parse(self, response):

        for item in response.css("div#raster>ul>li>a::attr(href)").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        verhuurd = "".join(response.xpath("//div[@class='new_text']/text()[contains(.,'verhuurd.')]").extract())
        if "verhuurd" in verhuurd:
            return
        item_loader.add_value("external_source", "Dochy_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            

        description = "".join(response.xpath(
            "//div[@class='new_text']//text()"
        ).getall())
        if description:
            item_loader.add_value("description", re.sub("\s{2,}", " ", description))
            if "garage" in description or "parkeerplaatsen" in description or "garagebox" in description:
                item_loader.add_value("parking", True)
            if "terras" in description:
                item_loader.add_value("terrace", True)
            if "balkon" in description:
                item_loader.add_value("balcony", True)
        rent = response.xpath("//strong[@itemprop='price']/text()").get()
        if rent:
            price = rent.replace(".", "").replace(",", ".")
            item_loader.add_value("rent",str(int(float(price)) ))
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath(
            "external_id",
            "//div[@id='cta_white']/ul/li[./span[.='Referentie']]/span[2]//text()",
        )
            
        square = response.xpath(
            "//div[@id='cta_white']/ul/li/span[contains(text(),'m²')]/text()"
        ).get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        room2 = ""
        room_count = response.xpath("//div[@class='placeholder p-ends-90']/ul/li/span[contains(.,'Slaapkamers')]/span[not(self::span[@class='bold'])]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        elif not room_count:
            room_count = response.xpath("//div[@class='new_text']//text()[contains(.,'slaapkamer')]").get()
            if room_count:


                room = room_count.split("slaapkamers")[0].split(",")[-1]
                if "slaapkamer(s)" in room_count:
                    room3 = room_count.split("slaapkamer(s)")[0].split(",")[-1].split("-")[-1]
                    item_loader.add_value("room_count",room3)

                if " " in room:
                    room2 = room.strip().split(" ")[0]
                else:
                    room2 = room

                
                if room2.isdigit():
                    item_loader.add_value("room_count",room2)     
        room = response.xpath("//h1/text()").re_first(r'(\d).*slaapkamer')
        if room:
            item_loader.add_value("room_count", room)  
        
        utilities = response.xpath(
            "//div[@class='placeholder p-ends-90']/ul/li/span[contains(.,'Provisie syndiek / maand')]/span[not(self::span[@class='bold'])]/text()"
        ).get()
        if utilities:
            utilities = utilities.replace(".", "").replace(",", ".")
            item_loader.add_value("utilities",str(int(float(utilities)) ))
        # property_type = response.xpath(
        #     "//span[@itemprop='name']/text()"
        # ).extract_first()
        bathroom_count = response.xpath("//div[@class='placeholder p-ends-90']/ul/li/span[contains(.,'Badkamer')]/span[not(self::span[@class='bold'])]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        else:
            bathroom = "".join(response.xpath("//div[@class='new_text']/text()[contains(.,'badkamer')]").extract())
            if bathroom:
                bathroom_c = bathroom.split("badkamers")[0].strip().split(" ")[-1].strip()
                if bathroom_c.isdigit():
                    item_loader.add_value("bathroom_count",bathroom_c)
                else:
                    if "badkamer" in bathroom:
                        bathroom_c2 = bathroom.split("badkamer")[0].strip().split(" ")[-1].strip()
                        if bathroom_c2.isdigit():
                            item_loader.add_value("bathroom_count",bathroom_c2)
                        else:
                            item_loader.add_value("bathroom_count","1")



        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_xpath(
            "available_date",
            "//div[@class='placeholder p-ends-90']/ul/li/span[contains(.,'Beschikbaar')]/span[not(self::span[@class='bold'])]/text()",
        )

        energy_label = response.xpath("substring-after(//div[@class='placeholder p-ends-90']/ul/li/text()[contains(.,'EPC')],': ')").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("kWh")[0])
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//a[@class='image-grid-element']/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        item_loader.add_xpath(
            "city", "normalize-space(//span[@itemprop='addressLocality'])"
        )

        item_loader.add_xpath(
            "address", "normalize-space(//span[@itemprop='addressLocality'])"
        )

        phone = response.xpath(
            '//div[@class="contactdata"]/a[1]/span[@class="new_text new_text--white"]/text()'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))

        email = response.xpath(
            '//div[@class="contactdata"]/a[3]/span[@class="new_text new_text--white"]/text()'
        ).get()
        if email:
            item_loader.add_value("landlord_email", email)

        item_loader.add_value("landlord_name", "IMMO DOCHY")
        
        
        yield item_loader.load_item()
