# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'rappange_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    def start_requests(self):
        headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.rappange.com"
        }
       
        data = {
            "sorteer": "Desc~Prijs",
            "prijs": "0,999999999",
            "prefilter": "Huuraanbod",
            "pagenum":"0",
            "pagerows":"36",
       }
       
        yield FormRequest(
            "https://www.rappange.com/huizen/smartselect.aspx",
            dont_filter=True,
            formdata=data,
            headers=headers,
            callback=self.jump,
        )
    
    def jump(self, response):
        data = json.loads(response.body)
        
        ids = ""
        for item_id in data["AllMatches"]:
            
            ids += item_id + ","
        
        form_data = {
            "id": ids.rstrip(",")
           
       }
       
        headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.rappange.com"
        }
       
        yield FormRequest(
            "https://www.rappange.com/huizen/smartelement.aspx",
            dont_filter=True,
            formdata=form_data,
            headers=headers,
            callback=self.parse,
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'object-element2')]"):
            follow_url = item.xpath(".//div[@class='object-adres']/a/@href").extract_first()
            prop = item.xpath(".//div[@class='object-feature' and contains(.,'Soort')]//div[contains(@class,'features-info')]/text()").extract_first()
            if "Garage" not in prop:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type" : "apartment"})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Rappange_PySpider_" + self.country + "_" + self.locale)
        
        title = "".join(response.xpath("//div[@class='object-adres']/h1//text()").extract())
        item_loader.add_value("title", title)
        item_loader.add_value("address", title)
        item_loader.add_value("city", "Amsterdam")

        item_loader.add_value("external_link", response.url)

        external_id = response.url.split('-')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        bathroom_count = response.xpath("//div[contains(text(),'Aantal badkamers')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        deposit  = response.xpath("//div[contains(text(),'Waarborgsom')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].replace('.', '').strip())
        
        energy_label = response.xpath("//div[contains(text(),'Energielabel')]/following-sibling::div/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip().split(' ')[-1].strip())

        price = response.xpath("//h2/text()").get()
        if price:
            item_loader.add_value("rent", price.split("€")[-1].strip().split(' ')[0].replace('.', ''))
            item_loader.add_value("currency", "EUR")
        else: return

        item_loader.add_value("property_type", response.meta.get("property_type"))

        square = response.xpath("//div[div[. ='Gebruiksoppervlak wonen']]/div[2]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        images = [response.urljoin(x)for x in response.xpath("//div[@id='photos']//div[@class='thumbnail']/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='photos']//div[@class='thumbnail']/a/img[contains(@src,'etage')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        room = response.xpath("//div[div[.='Totaal aantal kamers']]/div[@class='features-info col-12 col-xs-12 col-sm-7']//text()").extract_first()
        item_loader.add_value("room_count",room)

        available_date = response.xpath("//div[@class='object-feature'][2]//div[div[. ='Aanvaarding']]/div[2]/text()[. !='direct' and . !='in overleg' ]").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        desc = "".join(response.xpath("//div[@id='description']//text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc)

        if desc:
            if 'balkon' in desc.lower():
                item_loader.add_value("balcony", True)
            if 'lift' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'terras' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'vaatwasser' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)
            if 'etage' in desc.lower():
                parsed_text = desc.lower().split('etage')
                for i in range(len(parsed_text) - 1):
                    floor = "".join(filter(str.isnumeric, parsed_text[i].strip().split(' ')[-1]))
                    if floor.strip().isnumeric():
                        item_loader.add_value("floor", floor.strip())
                        break

        terrace = "".join(response.xpath("//div[div[. ='Soort parkeergelegenheid']]/div[2]/text()").extract())
        if terrace:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//div[div[. ='Specifiek']]/div[2]/text()").extract()).strip()
        if terrace:
            item_loader.add_value("furnished", True)

        item_loader.add_xpath("landlord_phone", "//p[@class='object-detail-contact-phone']/a/text()")
        item_loader.add_xpath("landlord_email", "//p[@class='object-detail-contact-email']/a/text()")
        item_loader.add_value("landlord_name", "Rappange")

        yield item_loader.load_item()