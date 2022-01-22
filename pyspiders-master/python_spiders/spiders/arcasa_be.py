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
    name = 'arcasa_be'
    execution_type='testing'
    country='belgium'
    locale='nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.arcasa.be/te-huur",
                ],
                "property_type" : "apartment",
            }
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def jump(self, response):
        property_type = response.meta.get("property_type")

        formdata = {
            "__EVENTTARGET": "ctl00$ContentControl$lnkSearch",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "ULq4ie2lmKwU+qFVBkK/1BwL5bPg6rWd+OG7OpgPxwbffznZJGdrmy0ZIkGb54zk2MdVxFKywlCrulNYHWd6vbOZycg1POM0RvccCo10wfFTTUzBgMdUsyOGc+YbO6alRs38ZN7uZEQHGIPZ3dcC0QhTW3bCwkTUPnBgP0GgO6Q1hiucfjbli4EPiqu2cStD+MpCudym3xG2REgAsN/k2KNeN69f4tlmOuVcDX063UaaCk2ZBhA4wQTo4QplHv+NRVEq6eqRNqHRrkregm2lrFY7Q5KmAS4GrPkMTyMonfZ8H82a4kRx8L1+GLJO5+Hpmv+fFTiU6YGvSOI7jYvGXxFdRP9m03FWKcBMhlJ94RyIFv6nQMY4ur5NlWpldPQCuu+WrV6QSxMJDMLxIjL1Ibbu2uuiz8CnlF8PjDIXUY886ghiEIK1sDWBf4U8tQEwq49j58eVFzWhuOyLnQubevjOeSLzONPq+O6d3Gu+suKNvt1O7wGDFfFBcixZK2WJqzloJBoF61OLLRF/FJtxi8O9fzP95YzMTAKnz459a42t2LNtgouuBFVspbX1ZRIMCVCsmvdn/O3HNRC1SDYdf9+aMxfEp5ePYIC5Vj4qsH5oxNkTpYeJTkKEAswx/AbvxwFCxSW91bwuArA+CRrjkrjHVvNTKdJizzkJZw8lCtMAhrvg7xxHTsnDSMEOkeLn",
            "__VIEWSTATEGENERATOR": "CE9C5ED1",
            "__EVENTVALIDATION": "Ux0/qNQDKgsaGPG4Vrebxs0lG3XVpmxNmJFfkhQ+Deq/Oz9rUsuFBCG7w4AQ4EWsH36GrJySzAzAlqQtM0iYhBQH1uky+QhsPEYhcp4Sr3U0Xw8pb8MXxcqnoSlQkXVdq9hhzNyaPQLJdqeNhaN4PR9NBxl1Jnzr8UfnEX/pojVkYMdMTWB98f1MUl6AmF/y6HyFXcO6OFQPQQO4j5MRb7TVQzMQKJv41cHKKIxzw+EYaG3t2eqFGQM1b8lcR0R7C61psDiF9kw8YVCxKrWOMKas2DJpFJ2KVCqZYOSrRveLRJppWeTm4KHafpOJ6SwMdRD46BPhrF5eEu6919C6VsY7m3PxpKgKymDJ93orfWIfHufV5uQdQlj/nzfqfe0pvEb8k+z7Nhqc8PJ5Kbck1lrOsFuLfo/qqmHpeMdzHx1Ab6Vn2MTqM112NRw+QDZmCWo6aLkKUtdBe/3q3qR6L14V22pXL0DdY3yIeZd++7EfWEv/2UZ6a1Qw9LnuyZKfnnhhYhG8Ua3upA3BCAxtuMaEnBCI668WbwznEwmkPJ7ddvpyilr4a+Rrt06kpOWkDDV1bytFDOmdO4OINihLhaGFVFxIYN12rtDeyRhY6D4FFVjqCDJ+S716swgIXr0TgQUZoIXOaz26KhqywmuE9GB95iUVp0hoPunmWRN5bh60EBp3WvCbsqOnrWl08IYuVA24kNWlFFJjmAZRAGRZ4w==",
            "ctl00$ContentControl$ddSort": "- - -",
            "ctl00$ContentControl$ddType": "2",
            "ctl00$ContentControl$ddGemeente": "- - -",
            "ctl00$ContentControl$ddPrijs": "- - -",
        }

        yield FormRequest(
            "https://www.arcasa.be/te-huur",  
            formdata=formdata, 
            callback=self.parse, 
            dont_filter=True, 
            meta={'property_type': property_type}
            )


    def parse(self, response):
        property_type = response.meta.get("property_type")
        for item in response.xpath("//div[@class='properties-huur']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "kantoor" in response.url:
            return
        item_loader.add_value("external_source", "Arcasa_PySpider_belgium_nl")
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()") 
        externalid=response.url
        if externalid:
            externalid=externalid.split("huur/")[-1].split("-")[0]
            item_loader.add_value("external_id",externalid)

        dontallow=response.xpath("//div[@class='row']/div/h3/span/../text()[last()]").get()
        if dontallow:
            dontallow=dontallow.replace("-","").replace("\n","").replace("\r","").strip()
            if dontallow and "binnenstaanplaats" in dontallow.lower():
                return

        rent =  "".join(response.xpath("normalize-space(//div[@id='ContentControl_pnlTitle']/div/div/div/h3/text())").extract())
        if rent:                
            price = rent.split("per")[0].strip().replace(",","").strip().replace(".","")
            if price != "":
                item_loader.add_value("rent_string", price)
            else:
                item_loader.add_value("currency", "EUR")

        address = " ".join(response.xpath("//div[@class='row']/div/p[contains(@class,'font-size22')]/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        city=response.xpath("//div[@class='row']/div/h3/span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("-","").replace("\n","").replace("\r","").strip())

        desc =  " ".join(response.xpath("//section[@class='projectintro']/div//p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
      

        availabledate= " ".join(response.xpath("//section[@class='projectintro']/div//p/text()").extract())
        if availabledate:
            availabledate=availabledate.split("vanaf")[-1].strip()
            if "heden" not in availabledate.lower():
                date_parsed = dateparser.parse(
                    availabledate, date_formats=["%m-%d-%Y"]
                )
                if date_parsed:
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)


        utilities =  "".join(response.xpath("substring-after(//section[@class='projectintro']/div//p/text()[contains(.,'kosten')],': ')").extract())
        if utilities and "X" not in utilities:            
            uti = utilities.replace("€","").strip().split(" ")[0].replace(",",".")
            item_loader.add_value("utilities", int(float(uti)))

        deposit =  "".join(response.xpath("substring-after(//section[@class='projectintro']/div//p/text()[contains(.,'Huurwaarborg')],': ')").extract())
        if deposit and price:           
            dep = deposit.split("maanden")[0].strip()         
            item_loader.add_value("deposit", int(dep)*int(price.replace("€","").strip()))

        meters =  "".join(response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,'Bewoonbare')]/span//text()").extract())
        if meters:       
            s_meters = meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(s_meters)))


        e_label =  "".join(response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,'EPC-klasse')]/span//text()").extract())
        if e_label:         
            item_loader.add_value("energy_label", e_label)
        else:
            label =  "".join(response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,' EPC (Kwh/m²/j)')]/span//text()").extract())
            if label:         
                item_loader.add_value("energy_label", str(int(float(label))))
           
        room = response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,' Slaapkamers')]/span//text()").extract_first()
        if room:
            item_loader.add_value("room_count", room)
        item_loader.add_xpath("bathroom_count", "//div[@class='row margin-25px-bottom']/div//div/p[contains(.,' Badkamers')]/span//text()")

        images = [ x for x in response.xpath("//div[@class='owl-carousel owl-theme listing-main-img']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  

        floor_images = [ x for x in response.xpath("//div[@class='row margin-25px-bottom']/div//a/@href").getall()]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)  

        terrace = "".join(response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,' Terras')]/span/strong/i/@class").extract())
        if terrace:
            if "check" in terrace:
                item_loader.add_value("terrace", True)

        elevator = "".join(response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,' lift')]/span/strong/i/@class").extract())
        if elevator:
            if "check" in elevator:
                item_loader.add_value("elevator", True)


        parking = "".join(response.xpath("//div[@class='row margin-25px-bottom']/div//div/p[contains(.,' Parking')]/span/strong/i/@class").extract())
        if parking:
            if "check" in parking:
                item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Arcasa Uw Vastgoedbemiddelaar")
        item_loader.add_value("landlord_phone", "+32 (0)3 877 70 70")
        item_loader.add_value("landlord_email", "info@arcasa.be")

        yield item_loader.load_item()