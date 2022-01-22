import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import math

class liv_rent_PySpider_canadaSpider(scrapy.Spider):
    name = 'liv_rent'
    allowed_domains = ['liv.rent']
    page_number = 2
    start_urls = [
        'https://beta.liv.rent/rental-listings?availability_date=NOW&cities=Toronto&cities=Vancouver&cities=Montreal&cities=Ottawa&cities=Calgary&cities=Edmonton&cities=Fort%20St%20John&cities=Victoria&cities=Regina&cities=Hamilton&cities=London&cities=Windsor&cities=Kitchener&cities=Oshawa&cities=Brampton&cities=Mississauga&cities=Kelowna&cities=Red%20Deer&cities=Surrey&cities=Coquitlam&cities=Richmond&page=1&search_only_verified=false',
        
    ]
    country = 'canada'
    locale = 'en-ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        urls = response.css("#__next > div.main-container__Container-sc-zq9sru-0.ictCvj > div:nth-child(3) > div.rental-listings__Wrapper-sc-5f66w5-1.leXTcz > div > div > div.list__Wrapper-sc-17iucdr-0.bAVRwq > div > a::attr(href)").extract()
        for url in urls:
            url="https://beta.liv.rent"+ url
            yield Request(url=url,
                          callback=self.parse_property)
        next_page = 'https://beta.liv.rent/rental-listings?availability_date=NOW&cities=Toronto&cities=Vancouver&cities=Montreal&cities=Ottawa&cities=Calgary&cities=Edmonton&cities=Fort%20St%20John&cities=Victoria&cities=Regina&cities=Hamilton&cities=London&cities=Windsor&cities=Kitchener&cities=Oshawa&cities=Brampton&cities=Mississauga&cities=Kelowna&cities=Red%20Deer&cities=Surrey&cities=Coquitlam&cities=Richmond&page=' + str(liv_rent_PySpider_canadaSpider.page_number) +'&search_only_verified=false'
        if liv_rent_PySpider_canadaSpider.page_number <= 146:
            liv_rent_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)
        
    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("#Overview > div > section > div.listing-summary__DesktopTitle-sc-1ds04v5-3.iRyscl > div > div > div.listing-summary__AddressWrapper-sc-1ds04v5-10.fFDGjL > h1::text").extract()
        external_id = response.url.split('/')[-1]
        try:
            title = title[0] + title[1]
        except:
            title = title[0]
            pass
        description = response.css("#__next > div.main-container__Container-sc-zq9sru-0.ictCvj > script:nth-child(4)::text").get().split('description\":\"')[1].split('\"')[0].split('please contact')[0].split('Please contact Mina')[0]
        city = title.split(', ')[-2]
        address = title
        property_type = response.css("#Overview > div > section > div.listing-summary__DesktopTitle-sc-1ds04v5-3.iRyscl > div > h4::text").get().split(' ')[-1].lower()       
        square_meters = response.css("#Overview > div > section > div.listing-summary__DetailsWrapper-sc-1ds04v5-4.bTKZnC > div:nth-child(3) > p::text").get().split(' ft')[0]
        if "Furnish Available" in square_meters:
            square_meters = None
        try:
            square_meters = square_meters.split('-')[0]
        except:
            pass
        try:
            square_meters = round(int(square_meters)/10.764,1)
        except:
            pass
        room_count = int(response.css("#Overview > div > section > div.listing-summary__DetailsWrapper-sc-1ds04v5-4.bTKZnC > div:nth-child(1) > p::text").get().split(' ')[0])
        try:
            info = response.css("#Overview > div > section > div.listing-summary__DetailsWrapper-sc-1ds04v5-4.bTKZnC > div:nth-child(1) > p::text").get()
            if "ft²" in info:
                room_count = 1
                square_meters = round(int(info.split(' ft²')[0])/10.764,1)
                furnished = response.css("#Overview > div > section > div.listing-summary__DetailsWrapper-sc-1ds04v5-4.bTKZnC > div:nth-child(3) > p::text").get()
        except:
            pass
        bathroom_count = response.css("#Overview > div > section > div.listing-summary__DetailsWrapper-sc-1ds04v5-4.bTKZnC > div:nth-child(2) > p::text").get().split(' ')[0]
        try:
            bathroom_count = math.floor(bathroom_count)
            bathroom_count = int(bathroom_count)
        except:
            pass
        images = response.css('div.photo-grid > div > div > button > div > noscript > img::attr(srcset)').extract()
        for i in range(len(images)):
            images[i] = images[i].split(' ')[-2]
        external_images_count = len(images)
        rent_x = response.css("#__next > div.main-container__Container-sc-zq9sru-0.ictCvj > div.page__TabletApplyFooterWrapper-sc-1fsjs7a-36.eAhoqq > div > div > div.page__StickyContentWrapper-sc-1fsjs7a-12.iKpIkZ > div.page__PriceColumn-sc-1fsjs7a-14.ghLcIR > div > p > span::text").get().split('$')[-1]
        if ',' in rent_x:
            rent_y = rent_x.split(',')[0]
            rent_z = rent_x.split(',')[-1]
            rent = rent_y + rent_z
            rent = int(rent)
        else:
            rent = int(rent_x)
        currency = "CAD"
        pets_allowed = response.css("#__next > div.main-container__Container-sc-zq9sru-0.ictCvj > div.page__ListingInfo-sc-1fsjs7a-3.iFMGcT > div > div.page__MobilePadding-sc-1fsjs7a-8.fyTHRs > section > div.page__DetailItem-sc-1fsjs7a-20.eFBeKJ > div:nth-child(1) > p.page__DetailText-sc-1fsjs7a-22.page__DetailTextBold-sc-1fsjs7a-25.Zohff.hygeeG::text").get()
        if pets_allowed is None:
            pets_allowed = response.css("#__next > div.main-container__Container-sc-zq9sru-0.ictCvj > div.page__ListingInfo-sc-1fsjs7a-3.iFMGcT > div > div.page__MobilePadding-sc-1fsjs7a-8.fyTHRs > section > div:nth-child(3) > p.page__DetailText-sc-1fsjs7a-22.page__DetailTextBold-sc-1fsjs7a-25.Zohff.hygeeG::text").get()
        if "Not allowed" in pets_allowed:
            pets_allowed = False
        else:
            pets_allowed = True
        try:
            furnished = response.css("#Overview > div > section > div.listing-summary__DetailsWrapper-sc-1ds04v5-4.bTKZnC > div:nth-child(4) > p::text").get()
        except:
            pass
        if 'Unfurnished' in furnished:
            furnished = False
        elif 'Furnished' in furnished or 'Furnish Available' in furnished:
            furnished = True
        lease_details = response.css("#Overview > div > div.lease-details__Wrapper-sc-138jmva-0.daQyuH.page__StyledLeaseDetails-sc-1fsjs7a-9.jKjZfE > section > div.lease-details__InnerWrapper-sc-138jmva-2.hwMDuZ > div > p::text").extract()
        amenities = response.css("#Amenities > section > div:nth-child(2) > div > div > div > p::text").extract()
        try:
            building_amenities = response.css("#Building > section > div.feature-list__Wrapper-sc-d8ptjn-0.ksXLwu > div > div > div > p::text").extract()
        except:
            pass
        if "Parking available" in lease_details:
            parking = True
        else:
            parking = False
        if "Elevator" in building_amenities:
            elevator = True
        else:
            elevator = False
        if 'balcony' in building_amenities or 'Large Balcony' in amenities:
            balcony = True
        else:
            balcony = False
        if 'Patio / Deck / Terrace' in building_amenities:
            terrace = True
        else:
            terrace = False
        if 'Indoor Pool' in building_amenities:
            swimming_pool = True
        else:
            swimming_pool = False
        if 'In Suite Washer' in amenities:
            washing_machine = True
        else:
            washing_machine = False
        if 'Dishwasher' in amenities:
            dishwasher = True
        else:
            dishwasher = False
        landlord_name = response.css("#Overview > div > section > div.listing-summary__TouchInfoWrapper-sc-1ds04v5-6.djnwzr > div > div > div > div.landlord-info__Column-sc-ndj10q-1.cXDwDj > div > div > p::text").get()
        landlord_email = 'info@liv.rent'
        landlord_phone = '(604) 593-3020'

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('address', address)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('images', images)
        item_loader.add_value('external_images_count', external_images_count)
        item_loader.add_value('rent', rent)
        item_loader.add_value('currency', currency)
        item_loader.add_value('pets_allowed', pets_allowed)
        item_loader.add_value('parking', parking)
        item_loader.add_value('balcony', balcony)
        item_loader.add_value('swimming_pool', swimming_pool)
        item_loader.add_value('washing_machine', washing_machine)
        item_loader.add_value('dishwasher', dishwasher)
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', landlord_email)
        item_loader.add_value('landlord_phone', landlord_phone)
        yield item_loader.load_item()