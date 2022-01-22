# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
import dateparser


class MySpider(Spider):
    name = 'sorimo_be'
    execution_type='testing'
    country = 'belgium'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.sorimo.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20true,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%200,%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%202%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D", "property_type": "apartment"},
	        {"url": "https://www.sorimo.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20true,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%200,%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D", "property_type": "house"} 
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@class='estate-list__item ']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            if page == 1:
                url = response.url.replace("FirstPage%22%3A%20true", "FirstPage%22%3A%20false")
                base_url = url
            else:
                base_url = response.meta.get("base_url")
                url = base_url.replace("PageNumber%22%3A%200",f"PageNumber%22%3A%20{str(page-1)}")
                
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Sorimo_Be_PySpider_"+ self.country)
        title = " ".join(response.xpath("//h1//text()").extract())
        price = ""
        if title:
            title = re.sub('\s{2,}', ' ', title)
            item_loader.add_value("title", title)
            rent = title.split("- ")[-1].strip()
            if "€" in rent and rent:
                price = rent.split("€")[0].replace("\xa0","")
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//h3[contains(.,'Adresse')]/following-sibling::p[1]/text()").get()
        if address:
            item_loader.add_value("address", address)
            
            zipcode = address.split(" - ")[-1].strip().split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)
        elif title:
            address = title.split("- ")[-2].strip()
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("address", address.replace("-"," "))
            item_loader.add_value("city", city.replace("-"," "))
            item_loader.add_value("zipcode", zipcode)
        
        room_count = response.xpath("//div[contains(@class,'detail')]/span/i[contains(@class,'bed')]/parent::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(@class,'detail')]/span/i[contains(@class,'bath')]/parent::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        square_meters = response.xpath("//div[contains(@class,'detail')]/span/i[contains(@class,'arrow')]/parent::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
        
        energy = response.xpath("//tr/th[contains(.,'PEB (classe)')]/following-sibling::td/text()").get()
        energy_label = response.xpath("//div[contains(@class,'detail')]/span/img[contains(@class,'peb')]/@alt[not(contains(.,'App'))]").get()
        if energy:
            item_loader.add_value("energy_label", energy)
        elif energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
        
        desc = "".join(response.xpath("//div/h2[contains(.,'Desc')]/..//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        # if "balcon" in desc.lower():
        #     item_loader.add_value("balcony", True)
        
        external_id = response.xpath("//tr/th[contains(.,'Référence')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        furnished = response.xpath("//tr/th[contains(.,'Meublé')]/following-sibling::td/text()").get()
        if furnished:
            if "Non" in furnished:
                item_loader.add_value("furnished", False)
            elif "Oui" in furnished:
                item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//tr/th[contains(.,'Terrasse')]/following-sibling::td/text()").get()
        if terrace:
            if "Non" in terrace:
                item_loader.add_value("terrace", False)
            elif "Oui" in terrace:
                item_loader.add_value("terrace", True)
            
        parking = response.xpath("//tr/th[contains(.,'Parking')]/following-sibling::td/text()").get()
        garage = response.xpath("//tr/th[contains(.,'Garage')]/following-sibling::td/text()").get()
        if parking or garage:
            if "Oui" in parking or "Oui" in garage:
                item_loader.add_value("parking", True)
            elif "Non" in parking or "Non" in garage :
                item_loader.add_value("parking", False)
        
        elevator = response.xpath("//tr/th[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator:
            if "Non" in elevator:
                item_loader.add_value("elevator", False)
            elif "Oui" in elevator:
                item_loader.add_value("elevator", True)
        
        available_date = response.xpath("//tr/th[contains(.,'Disponib')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%m/%d/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        utilities = response.xpath("//tr/th[contains(.,'Charge')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//tr/th[contains(.,'Garantie')]/following-sibling::td/text()").get()
        if price and deposit:
            item_loader.add_value("deposit", str(int(deposit)*int(price)))
        
        
        images = [ x for x in response.xpath("//div[@class='item']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "SORIMO")
        
        phone = response.xpath("//div[contains(@class,'agent')]/a/@href[contains(.,'tel')]").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:",""))
        
        item_loader.add_value("landlord_email", "info@sorimo.be")

         
        yield item_loader.load_item()