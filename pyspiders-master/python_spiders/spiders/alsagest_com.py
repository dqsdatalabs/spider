# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'alsagest_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.alsagest.com/fr/liste.htm?ope=2&filtre=2", "property_type": "apartment"},
	        {"url": "http://www.alsagest.com/fr/liste.htm?ope=2&filtre=8", "property_type": "house"},
        ]  # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'liste-bien-photo-frame')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Alsagest_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Ref')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//meta[contains(@property,'title')]//@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(.,'Ville')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[contains(.,'Ville')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface')]//parent::li/text()", input_type="F_XPATH", get_num=True, split_list={"m":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(.,'garantie')]//following-sibling::span[contains(@class,'cout')]/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'thumbs-flap-container')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(.,'provisions sur charges')]//following-sibling::span[contains(@class,'cout_charges_mens')]/text()[.!='0']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//li[contains(@class,'gg-map-marker-lat')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//li[contains(@class,'gg-map-marker-lng')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="ALSAGEST", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0389531835", input_type="VALUE")

        email = response.xpath("normalize-space(//div[@id='BlocFormDetailContact']/div/span[contains(.,'mail')]/text())").extract_first()
        if email:
            item_loader.add_value("landlord_email", email)

        rent = "".join(response.xpath("//p//text()").getall())
        if rent and "loyer"in rent.lower():
            rent = rent.lower().split("loyer")[1].split("€")[0].replace(":","").replace(",",".").strip()
            item_loader.add_value("rent", int(float(rent)))

        room_count = "".join(response.xpath("//span[contains(.,'Chambres')]//parent::li/text()").getall())
        if room_count:
            if room_count.strip().isdigit():
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count = "".join(response.xpath("//span[contains(.,'Pièce')]//parent::li/text()").getall())
                if room_count.strip().isdigit():
                    item_loader.add_value("room_count", room_count.strip())
        
        energy_label = response.xpath("//div[contains(@id,'TitreDpe')]//following-sibling::img//@src[contains(.,'nrj-w')]").get()
        if energy_label:
            energy_label = energy_label.split("nrj-w-")[1].split("-")[0]
            item_loader.add_value("energy_label", energy_label)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p//text()").getall())
        if available_date and "disponible" in available_date.lower():
            available_date = available_date.lower().split("disponible ")[1]
            if "de suite" not in available_date:
                available_date = available_date.replace("fin","").replace("en","").split(".")[0].strip()
                if "/" in available_date:
                    available_date = available_date.replace("le","").strip().split(" ")[0].replace("dpe","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        yield item_loader.load_item()