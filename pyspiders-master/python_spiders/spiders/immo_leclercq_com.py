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
class MySpider(Spider):
    name = 'immo_leclercq_com'
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    }
    
    def start_requests(self):
        formdata = {
            "PageID": "0",
            "SelectedType": "2",
            "SelectedCategory": "1",
            "EstateRef": "",
        }
        yield FormRequest(
            "https://www.immo-leclercq.com/fr",
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            headers=self.headers,
            meta={
                "property_type":"house"
            }

        )
       
    # 1. FOLLOWING
    def parse(self, response):
        with open("debug", "wb") as f:f.write(response.body)
        page = response.meta.get("page", 0)
        seen = False
        post_count = response.meta.get("post_count", 0)

        for item in response.xpath("//a[@class='estate-card']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 0 or seen:
            p_url = f"https://www.immo-leclercq.com/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20false,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%20{page},%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%202%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]}
            )
        else:
            if post_count == 0:
                formdata = {
                    "PageID": "0",
                    "SelectedType": "2",
                    "SelectedCategory": "2",
                    "EstateRef": "",
                }
                yield FormRequest(
                    "https://www.immo-leclercq.com/fr",
                    callback=self.parse,
                    formdata=formdata,
                    dont_filter=True,
                    headers=self.headers,
                    meta={
                        "property_type":"apartment",
                        "post_count" : post_count+1
                    }

                )        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        prop_type = response.xpath("//div[span[contains(.,'Catégorie')]]/span[2]//text()").extract_first()
        if prop_type:
            if "studio" in prop_type.lower():
                property_type = "studio"

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Immo_Leclercq_PySpider_belgium")

        title = " ".join(response.xpath("//h1//text()").extract())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title",title) 
        
        available_date = response.xpath("//tr[th[contains(.,'Disponibilité')]]/td/text()").extract_first() 
        if available_date and ("immédiatement" not in available_date.lower() or "now" not in available_date.lower()):
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        address = response.xpath("//h3[contains(.,'Adresse')]/following-sibling::p/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())
            address = address.split("-")[-1].strip().split(" ")
            item_loader.add_value("city", address[0])
            item_loader.add_value("zipcode", address[1])

        item_loader.add_xpath("external_id", "//tr[th[contains(.,'Référence')]]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[contains(.,'Salle')]]/td/text()")
        item_loader.add_xpath("floor", "//tr[th[contains(.,'Étage')]]/td/text()")

        room_count =response.xpath("//tr[th[contains(.,'chambre')]]/td/text()").extract_first()
        if room_count:                
            item_loader.add_value("room_count",room_count) 
        elif "studio" in property_type:
            item_loader.add_value("room_count","1") 

        rent =response.xpath("//h1/text()[last()]").extract_first()
        if rent:
            rent = rent.replace(" ","").replace("\r\n","").replace("€","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

        utilities =response.xpath("//tr[th[contains(.,'Charge')]]/td/text()").extract_first()
        if utilities:     
            item_loader.add_value("utilities", utilities) 

        square = response.xpath("//tr[th[contains(.,'habitable')]]/td/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        energy_label =response.xpath("//tr[th[contains(.,'PEB (classe)')]]/td/text()").extract_first()    
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())  

        furnished =response.xpath("//tr[th[contains(.,'Meublé')]]/td/text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        parking =response.xpath("//tr[th[contains(.,'Parking')]]/td/text()").extract_first()       
        if parking:
            if "non" in parking.lower():
                parking =response.xpath("//tr[th[contains(.,'Garage')]]/td/text()").extract_first()       
                if parking:
                    if "non" in parking.lower():
                        item_loader.add_value("parking", False)
                    else:
                        item_loader.add_value("parking", True)           
            else:
                item_loader.add_value("parking", True)
        
        terrace =response.xpath("//tr[th[contains(.,'Terrace')]]/td/text()").extract_first()    
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
    
        elevator =response.xpath("//tr[th[contains(.,'Ascenseur')]]/td/text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        dishwasher =response.xpath("//tr[th[contains(.,'Lave-vaisselle')]]/td/text()").extract_first()    
        if dishwasher:
            if "non" in dishwasher.lower():
                item_loader.add_value("dishwasher", False)
            else:
                item_loader.add_value("dishwasher", True)  

        desc = " ".join(response.xpath("//div[@class='col-md-9']/p[1]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='owl-estate-photo']//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        deposit = response.xpath("//tr[th[contains(.,'Garantie')]]/td/text()").get()
        if deposit:
            item_loader.add_value("deposit", int(deposit)*int(rent))
        
        item_loader.add_value("landlord_name", "IMMO LECLERCQ S.P.R.L.")
        item_loader.add_value("landlord_phone", "+32 69 891 891")
        item_loader.add_value("landlord_email", "contact@immo-leclercq.com")
        
        yield item_loader.load_item()

