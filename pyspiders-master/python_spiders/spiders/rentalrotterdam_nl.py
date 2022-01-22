# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'rentalrotterdam_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    

    def start_requests(self):
        start_urls = [
            {"url": "https://www.rentalrotterdam.nl/woningaanbod/huur/type-appartement", "property_type": "apartment"},
            {"url": "https://www.rentalrotterdam.nl/woningaanbod/huur/type-woonhuis", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 10)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'col-md-12 object_list')]//article//div[@class='object_address']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 10 or seen:
            headers = {
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
                "origin": "https://www.rentalrotterdam.nl"
            }
           
            data = {
                 "forsaleorrent": "FOR_RENT",
                  "take": "10",
                  "skip":f"{page}"
            }
           
            url = "https://www.rentalrotterdam.nl/0-2ac6/aanbod-pagina"
            yield FormRequest(
                url,
                formdata=data,
                headers=headers,
                callback=self.parse,
                meta={"page":page+10, 'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        parking = response.xpath("//div[@class='description textblock']//text()[contains(.,'PARKEERPLAATSEN') or contains(.,'Parkeerplaats')]").get()
        if not parking:
            item_loader.add_value("external_source", "Rentalrotterdam_PySpider_" + self.country + "_" + self.locale)

            title = response.xpath("//h1/text()").get()
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)
            item_loader.add_value("external_link", response.url)

            available_date ="".join(response.xpath("//tr[./th[.='Aanvaarding']]/td/text()[. !='Direct' and . !='In overleg']").extract())
            if available_date:
                yearly = available_date.split(" ")[-1]
                monthly =  available_date.split(" ")[-2]
                date =  available_date.split(" ")[-3]
                ava = "{} {} {}".format(date,monthly,yearly)
                date_parsed = dateparser.parse(ava, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            else:
                from datetime import datetime
                from datetime import date
                #import dateparser
                available_date = response.xpath("//td[contains(.,'Aangeboden sinds')]/following-sibling::td/text()").get()
                if available_date:
                    date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
                    today = datetime.combine(date.today(), datetime.min.time())
                    if date_parsed:
                        result = today > date_parsed
                        if result == True:
                            date_parsed = date_parsed.replace(year = today.year + 1)
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        

            price = response.xpath("//span[@class='object_price']/text()[contains(., '€')]").extract_first()
            if price:
                item_loader.add_value("rent", price.split("€")[1].strip().split(",")[0])
                item_loader.add_value("currency", "EUR")
            
            deposit = response.xpath("//tr[td[contains(.,'Borg')]]/td[2]/text()[contains(., '€')]").extract_first()
            if deposit:
                item_loader.add_value("deposit", deposit.split("€")[1].strip().split(",")[0])

            utilities = response.xpath("//tr[./td[.='Servicekosten' or .='Service' ]]/td[2]//text()").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€")[1].strip().split(",")[0])

            item_loader.add_value("property_type", response.meta.get('property_type'))

            square = response.xpath("//tr[./th[.='Gebruiksoppervlakte wonen']]/td/text()").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0])

            images = [response.urljoin(x)for x in response.xpath("//div[@id='object-photos']/a/@href").extract()]
            if images:
                    item_loader.add_value("images", images)

            room_count = response.xpath("//tr[td[.='Aantal kamers']]/td[2]/text()").extract_first()
            if room_count:        
                if 'slaapkamer' in room_count.lower():
                    item_loader.add_value("room_count", room_count.lower().split("slaapkamer")[0].strip().split(' ')[-1].strip())
                else:
                    item_loader.add_value("room_count", room_count.split("(")[0])

            desc = "".join(response.xpath("//div[@class='description textblock']//text()").extract())
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc.strip())

            item_loader.add_xpath("external_id", "//tr[td[.='Referentienummer']]/td[2]/text()")
            item_loader.add_xpath("floor","//tr[./td[.='Aantal bouwlagen']]/td[2]/text()")

            terrace = response.xpath("//tr[td[.='Inrichting']][2]/td[2]/text()").get()
            if terrace:
                item_loader.add_value("furnished", True)

            dishwasher =  "".join(response.xpath("//div[contains(@class,'description')]//text()[contains(.,'vaatwasser') or contains(.,'dishwasher') or contains(.,'vaatwasmachine')]").extract())
            if  dishwasher:
                item_loader.add_value("dishwasher", True)   

            balcony =  "".join(response.xpath("//tr[td[.='Heeft een balkon']]/td[2]/text()").extract())
            if  balcony:
                if "Ja" in balcony:
                    item_loader.add_value("balcony", True)

            address = response.xpath("//h1/text()").get()
            if address:
                address = address.split(':')[-1].strip()
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", address.split(',')[-1].strip().split(' ')[0].strip())
                item_loader.add_value("city", address.split(',')[-1].strip().split(' ')[-1].strip())

            latlng = "".join(response.xpath("//script[@type = 'text/javascript']/text()").extract())
            if latlng:
                lat =  latlng.split("center: [")[1].split(",")[0].strip()
                lng = latlng.split("center: [")[1].split("]")[0].split(",")[1].strip()
                item_loader.add_value('latitude',lat)
                item_loader.add_value('longitude', lng)


            item_loader.add_value("landlord_phone", "010 200 12 22")
            item_loader.add_value("landlord_email", "info@RentalRotterdam.nl")
            item_loader.add_value("landlord_name", "Rental Rotterdam")
            
            yield item_loader.load_item()