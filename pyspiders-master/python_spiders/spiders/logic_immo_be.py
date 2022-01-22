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
 
class MySpider(Spider): 
    name = 'logic_immo_be'  
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source="Logic_Immo_PySpider_belgium"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.logic-immo.be/en/rent/apartments-for-rent/belgium.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.logic-immo.be/en/rent/houses-for-rent/belgium.html",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

        # url = "https://www.logic-immo.be/en/announce/rent/apartment-for-rent/1650419"
        # yield Request(url, callback=self.populate_item, meta={"property_type":"house"})
    
    def jump(self, response):
        for item in response.xpath("//div[@class='mb-12']/div[@class='mb-8']"):
            url = response.urljoin(item.xpath("./a/@href").extract_first())
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='mb-12']/div[@class='mb-8']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        next_page = response.xpath("//li[@class='pagination-btn'][last()]/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        # with open("debug","wb") as f:f.write(response.body)
        rentcheck=response.xpath("//li//span[contains(.,'Rented')]/text()").get()
        if rentcheck:
            return

        prop_type = response.xpath("//h1/text()").get()
        if prop_type and "studio" in prop_type.lower(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//div/@data-address").get()
        if address:
            item_loader.add_value("address", address)
        else:
            address = " ".join(response.xpath('//p[@class="text-md mb-6 font-semibold"]/text()').getall())
            if address:
                item_loader.add_value("address", re.sub(r'\s{2}', '', address).strip())                 
            else:
                address = "".join(response.xpath("//h1//text()").getall())
                if address and "," in address:
                    address = address.split(",")[0].strip().split(" ")[-1]
                    item_loader.add_value("address", address)
                elif " in " in address:
                    address = address.split(" in ")[1].strip().split(" ")[0]
                    item_loader.add_value("address", address)
        
        city = " ".join(response.xpath('//p[@class="text-md mb-6 font-semibold"]/text()').getall())
        city2 = ""
        if city:
            city = city.split()[-1]
            if "-" in city:
                city = " ".join(city.split("-")[-2:])
                item_loader.add_value('city', city)
            else:
                city2 = city
        else:
            city = address
            if "-" in city:
                city = " ".join(city.split("-")[-2:])
                item_loader.add_value('city', city)
            else:
                city2 = address
        zipcode = response.xpath("//p[@class='text-md mb-6 font-semibold']/text()").re_first(r'\d{4}')
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        zipcheck=item_loader.get_output_value("zipcode")
        if not zipcheck:
            zipcode=response.xpath("//title//text()").get()
            if zipcode:
                item_loader.add_value("zipcode",zipcode.split(" ")[-2])
        citycheck=item_loader.get_output_value("city")
        if not citycheck: 
            city=response.xpath("//title//text()").get()
            if city: 
                item_loader.add_value("city",city.split(" ")[-1])
                
        if city2 and " " in city2 and "rent" in city2:
            city2 = city2.split("rent")[-1].strip()
            item_loader.add_value("city", city2)
        
        addrescheck=item_loader.get_output_value("address")
        if not addrescheck:
            addres=response.xpath("//title//text()").get()
            if addres:
                item_loader.add_value("address",addres.split(" ")[-1])
        
        rent = "".join(response.xpath("//td[contains(.,'Monthly rent')]//following-sibling::td//text()").getall())
        if rent:
            rent=rent.replace("€","").replace(" ","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = "".join(response.xpath("//ul[@class='announce-title-list']/li[@class='announce-title-list-element']/text()[contains(.,'room')]").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split("room")[0].replace("s",""))
        
        bathroom_count = response.xpath("//li[@class='li-PillBox-pill']/span[@class='li-PillBox-label']/text()[contains(.,'bathroom')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
         
        square_meters = "".join(response.xpath("//ul[@class='announce-title-list']/li[@class='announce-title-list-element']/text()[contains(.,'m²')]").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split("m²")[0])
        else:
            square_meters = response.xpath("//img[contains(@src,'area')]/following-sibling::p/text()[contains(.,'m²')]").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
        
        energy_label = "".join(response.xpath("//p[@class='text-md mb-4 font-semibold']/strong/text()").getall())
        item_loader.add_value("energy_label", energy_label.split("/")[0].strip())
        
        description = " ".join(response.xpath("//section/div[@class='text-smx mb-8 font-semibold break-words']/text()").getall())
        if description:
            description = re.sub(r'\s{2,}', ' ', description.strip()) 
            item_loader.add_value("description", description)

        image = " ".join(response.xpath("//script[contains(.,'large:')]/text()").extract())
        if image:
            img = []
            images = []
            image = image.split('fr:{large:"')
            for i in image:
                img.append(i.split('"')[0])
                for im in img:
                    if "https:\\u002F\\u002Fimgp" in im:
                        images.append(im.replace("\\u002F","/"))          
            item_loader.add_value("images", images)
        imagecheck=item_loader.get_output_value("images")
        if not imagecheck:
            image = " ".join(response.xpath("//script[contains(.,'large:')]/text()").extract())
            if image:
                img = [] 
                images = []
                image = image.split('{large:"')
                for i in image:
                    img.append(i.split('"')[0])
                    for im in img:
                        if "https:\\u002F\\u002Fimgp" in im:
                            images.append(im.replace("\\u002F","/"))          
                item_loader.add_value("images", images)
                imagescheck1=item_loader.get_output_value("images")
                if not imagescheck1:
                    image = " ".join(response.xpath("//script[contains(.,'large:')]/text()").extract())
                    if image:
                        img = [] 
                        images = []
                        image=image.split("image{:err")[-1]
                        image = image.split('https:')
                        for i in image:
                            img.append(i.split('"')[0])
                            for im in img:
                                if "\\u002F\\u002Fimgp" in im and not "logo" in im:
                                    im="https:"+im
                                    images.append(im.replace("\\u002F","/"))  
                                           
                        item_loader.add_value("images", images)


        item_loader.add_xpath("latitude", "//div/@data-latitude")
        item_loader.add_xpath("longitude", "//div/@data-longitude")
        
        external_id = "".join(response.xpath("//div[@class='li-Detail-colsPri']/div/div[contains(@class,'items-baseline')]/text()").getall())
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        parking = response.xpath("//img[contains(@src,'garage')]/@src").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[@class='li-PillBox-pill']/span[@class='li-PillBox-label']/text()[contains(.,'elevator')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//img[contains(@src,'terrace')]/@src | //li[contains(.,'Terrace')]//span/@class[not(contains(.,'remove'))]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//img[contains(@src,'balcon')]/@src").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        floor = response.xpath("//li[@class='li-PillBox-pill']/span[@class='li-PillBox-label']/text()[contains(.,'floor')]").get()
        if floor:
            item_loader.add_value("floor", floor.strip().split(" ")[0])
        
        utilities = response.xpath("//td[contains(.,'Charges')]//following-sibling::td//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(" ","")
            if utilities.isdigit() and int(utilities) > 0:
                item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'charge')]/span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]//span/@class[not(contains(.,'remove'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Swimming')]//span/@class[not(contains(.,'remove'))]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_name", "Logic-Immo.be")
        item_loader.add_value("landlord_email", "klanten@ipm-immo.be")
        item_loader.add_value("landlord_phone", "+32 (0)2 340 70 70")

        
        yield item_loader.load_item()