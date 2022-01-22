# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader 
from ..helper import *
import re
import dateparser

class PrimmoSpider(scrapy.Spider): 
    name = "primmo"
    allowed_domains = ["primmo.be"]
    start_urls = ("http://www.primmo.be/index.php?action=list&ctypmandatmeta=l&ctypmeta=appt&llocalite=&mprixmin=&mprixmax=&cbien=","http://www.primmo.be/index.php?action=list&ctypmandatmeta=l&ctypmeta=mai&llocalite=&mprixmin=&mprixmax=&cbien=")
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        page = response.meta.get('page', 1)
        seen = False
        for item_responses in response.xpath(".//div[@class='enfant']/article"):
            link = item_responses.xpath("./figure/a/@href").get()
            if link:
                pr_type = item_responses.xpath("./div[@class='details']/h4/a/text()").get()
                if pr_type in ["Studio", "Appartement", "Maison"]:
                    if "Maison" in pr_type:
                        pr_type = "house"
                    elif "Studio" in pr_type:
                        pr_type = "studio"
                    else:
                        pr_type = "apartment"
                    yield scrapy.Request(
                        response.urljoin(link),
                        self.parse_detail,
                        cb_kwargs=dict(property_type=pr_type),
                    )
                    seen = True
        if page == 1 or seen:
            url = f"http://www.primmo.be/index.php?page={page}&action=list&ctypmandatmeta=l&ctypmeta=appt&llocalite=&mprixmin=&mprixmax=&cbien=#toplist"
            yield scrapy.Request(url, callback=self.parse, meta={"page": page+1})
        

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        external_id="".join(response.xpath("//div[@id='textbox']/p[2]/text()").get())
        if external_id:
            external_id=external_id.split(":")[-1]
            item_loader.add_value("external_id",external_id)
        title="".join(response.xpath("//h2/text()").getall())
        if title:
            title=title.replace("\n","").replace("\t","").strip() 
            item_loader.add_value("title",title)   
        address ="".join(response.xpath("//h2/text()").getall())
        if address:
            address=address.replace("\n","").replace("\t","").strip() 
            item_loader.add_value("address",address.split("-")[-1])
            city=address.split(" - ")[-1]
            item_loader.add_value("city",city)
            # zipcode = address.split(",")[-1].strip()
            # item_loader.add_value("zipcode",zipcode)

        rent = response.xpath("//li[contains(.,'Prix')]/text()").get()
        if rent:
            rent=rent.split(":")[-1].replace(".","")
            rent=re.findall("\d+",rent)
            item_loader.add_value("rent", rent)   
            item_loader.add_value("currency", "EUR") 
        # deposit =response.xpath("//span[contains(.,'Deposit:')]/following-sibling::text()").get()
        # if deposit:
        #     deposit=deposit.replace("£","").strip()
        #     item_loader.add_value("deposit", deposit)
        # parking =response.xpath("//span[contains(.,'Parking:')]/following-sibling::text()").get()
        # if parking:
        #     item_loader.add_value("parking", True)  
   
        room_count =response.xpath("//li[contains(.,'Chambre')]/text()").get()
        if room_count:   
            room_count=re.findall("\d+",room_count)
            item_loader.add_value("room_count",room_count)
        bathroom_count =response.xpath("//li[contains(.,'Salle de bains')]/text()").get()
        if bathroom_count:  
            bathroom_count=re.findall("\d+",bathroom_count) 
            item_loader.add_value("bathroom_count",bathroom_count)
     
        images =[ x for x in response.xpath("//div[contains(@class,'fotorama')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            squ=re.findall("\d+",square_meters)
            item_loader.add_value("square_meters", squ)
            

        # lat=response.xpath("//div[@id='property-map']/@data-lat").get()
        # if lat:
        #     item_loader.add_value("latitude", lat)
        # lng=response.xpath("//div[@id='property-map']/@data-lng").get()
        # if lng:
        #     item_loader.add_value("longitude", lng)
       
        desc = response.xpath("//div[@id='desc']/p/text()").get()
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_value("landlord_name", "PRIMMO-PERUWELZ")
        item_loader.add_value("landlord_phone", "069 22 78 11")
        item_loader.add_value("landlord_email", "clementine@primmo.be")  
    
        yield item_loader.load_item()

