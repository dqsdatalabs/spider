import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import math


class DelaneypropertiesltdComSpider(scrapy.Spider):
    name = 'delaneypropertiesltd_com'
    allowed_domains = ['delaneypropertiesltd.com']
    start_urls = ['https://delaneypropertiesltd.com/rental-properties']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("body > div.rh_wrap > section.rh_section.rh_section--flex.rh_wrap--padding.rh_wrap--topPadding > div.rh_page.rh_page__listing_page.rh_page__main > div.rh_page__listing > article"):
            url = appartment.css(
                "div.rh_list_card__details > h3 > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.css('body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__main > div > div.rh_property__row.rh_property__meta.rh_property--borderBottom > div.rh_property__id > p.id::text').get().strip()

        title = response.css(
            'body > div.rh_wrap > section.rh_banner.rh_banner__image > div.rh_banner__wrap > h2::text').get()
        address = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_page__head.rh_page__property > div.rh_page__property_title > p::text').get()
        zipcode = address.split(',')[-2]

        rent = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_page__head.rh_page__property > div.rh_page__property_price > p.price::text').get().split("$")[1].split(" ")[0]

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        description = ''
        description_array = response.css(
            "body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__main > div > div.rh_content > p::text").extract()

        for item in description_array:
            description += item

        images = response.css(
            'a.swipebox > img::attr(src)').extract()

        feats = response.css('body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__main > div > div.rh_property__row.rh_property__meta_wrap > div')

        bedrooms = None
        bathrooms = None
        space = None
        pets = None
        parking = None
        available_date = None
        for item in feats:
            try:
                if "Bedrooms" in item.css('span.rh_meta_titles::text').get():
                    bedrooms = item.css('span.figure::text').get()
                elif "Bathrooms" in item.css('span.rh_meta_titles::text').get():
                    bathrooms = math.ceil(
                        float(item.css('span.figure::text').get()))
                elif "Garage" in item.css('span.rh_meta_titles::text').get():
                    parking = True
                elif "Pets" in item.css('span.rh_meta_titles::text').get():
                    pets = True
                    if "no" in item.css('span.figure::text').get().lower():
                        pets = False
                elif "Availability" in item.css('span.rh_meta_titles::text').get():
                    available_date = item.css('span.figure::text').get()
                elif "Area" in item.css('span.rh_meta_titles::text').get():
                    space = item.css(
                        'span.figure::text').get().strip().split(" ")[0]
                    space = int(int(space)/10.7639)
            except:
                pass

        features = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__main > div > div.rh_property__features_wrap > ul > li')

        elevator = None
        dish_washer = None
        laundry = None
        for item in features:
            if "Elevator" in item.css('a::text').get():
                elevator = True
            elif "Dishwasher" in item.css('a::text').get():
                dish_washer = True
            elif "Washer" in item.css('a::text').get():
                laundry = True

        landlord_name = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__sidebar > aside > section.widget.rh_property_agent > h3::text').get()
        landlord_phone = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__sidebar > aside > section.widget.rh_property_agent > div.rh_property_agent__agent_info > p.contact.mobile > a::text').get()
        landlord_email = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_property > div.rh_property__wrap.rh_property--padding > div.rh_property__sidebar > aside > section.widget.rh_property_agent > div.rh_property_agent__agent_info > p.contact.email > a::text').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("room_count", int(bedrooms))
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("square_meters", int(int(space)*10.764))
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("dishwasher", dish_washer)
        item_loader.add_value("washing_machine", laundry)
        item_loader.add_value("parking", parking)
        item_loader.add_value("pets_allowed", pets)
        item_loader.add_value("available_date", available_date.strip())

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()
