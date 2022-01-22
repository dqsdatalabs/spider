# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader

class placedorchesterappartements(scrapy.Spider):
    name = "placedorchesterappartements"
    allowed_domains = ["www.placedorchesterappartements.com"]
    start_urls = ['https://www.placedorchesterappartements.com/floorplans.aspx']
    country = 'canada'  # Fill in the Country's name
    locale = 'fr'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        head = {
            "User-Agent": 'tutorial (+http://www.yourdomain.com)'}
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET",meta={'dont_merge_cookies': True},headers=head, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        props = response.xpath('//div[@class="tab-pane row-fluid"]').extract()
        ptype = response.xpath('//table[@class="table"]/tr[1]/td[2]/text()').extract()
        description ="".join(response.xpath('//*[@id="CustMsgDivBottom"]/donottranslate/p/text()').extract())
        b_count=response.xpath('//table[@class="table"]/tr[2]/td[2]/text()').extract()
        sqft=response.xpath('//table[@class="table"]/tr[3]/td[2]/text()').extract()
        r=response.xpath('//table[@class="table"]/tr[4]/td[2]/text()[1]').extract()
        titles="".join(response.xpath('.//div[@class="span6"]/h2[@class="floor-plan-name"]/text()').extract()).strip().split("\r\n            ")
        plan=response.xpath('//div[@class="span6 text-center"]/img/@data-src').extract()
        room_count=None
        for i in range(len(props)):
            item_loader = ListingLoader(response=response)
            floor_plan_images = plan[i].replace("(1)","")
            title=titles[i].replace("\r\n            ","")
            pp=ptype[i]
            if pp =="Studio" :
                property_type="studio"
                room_count=1
            else :
                property_type="apartment"
                room_count=int(pp)
            bathroom_count=b_count[i]
            square_meters=sqft[i]
            if square_meters.isnumeric():
                pass
            else :
                continue
            rent = r[i]
            if rent =="Call for Details" :
                continue
            else :
                rent=rent.replace("$","").replace(",","").replace("-","")
            # # MetaData
            item_loader.add_value("external_link", response.url+f"#{i}")  # String
            item_loader.add_value("external_source", self.external_source)  # String
            # item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String
            # # Property Details
            item_loader.add_value("city", "Montreal")  # String
            # item_loader.add_value("zipcode", zipcode)  # String
            # item_loader.add_value("address", address)  # String
            # item_loader.add_value("latitude", latitude)  # String
            # item_loader.add_value("longitude", longitude)  # String
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type",
                                  property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # item_loader.add_value("available_date", available)  # String => date_format

            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", True)  # Boolean
            # item_loader.add_value("elevator", elevator)  # Boolean
            # item_loader.add_value("balcony", balcony)  # Boolean
            # item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            # item_loader.add_value("washing_machine", washing_machine)  # Boolean
            # item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images
            # item_loader.add_value("images", images)  # Array
            # item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", int(rent))  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "CAD")  # String
            #
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", "Place Dorchester")  # String
            item_loader.add_value("landlord_phone", "(514) 613-4690")  # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()




